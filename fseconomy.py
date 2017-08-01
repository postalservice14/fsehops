import pandas as pd
from io import StringIO
from urllib.request import urlopen
from math import radians
import pickle
import numpy as np
from pulp import LpMaximize, LpProblem, LpVariable
import time

import common
import const


class TooManyConnectionsException(Exception):
    pass


class ServerUnreachableException(Exception):
    pass


class FSEconomy(object):
    def __init__(self, local, service_key=None):
        self.airports = common.load_airports()
        self.aircraft = common.load_aircraft()
        self.last_request_time = time.time()
        self.service_key = service_key
        if local:
            self.assignments = common.load_pickled_assignments()
        else:
            self.assignments = self.get_assignments()

    def get_assignments(self):
        assignments = pd.DataFrame()

        i = 0
        lim = len(self.airports)
        while i + 1500 < lim:
            data = StringIO(self.get_jobs_from(self.airports.icao[i:i + 1500]))
            assignments = pd.concat([assignments, pd.DataFrame.from_csv(data)])
            i += 1500
            print(i)
        data = StringIO(self.get_jobs_from(self.airports.icao[i:lim - 1]))
        assignments = pd.concat([assignments, pd.DataFrame.from_csv(data)])
        with open('assignments', 'wb') as f:
            pickle.dump(assignments, f)
        return assignments

    def get_aggregated_assignments(self):
        grouped = self.assignments.groupby(['FromIcao', 'ToIcao', 'UnitType'], as_index=False)
        aggregated = grouped.aggregate(np.sum)
        return aggregated.sort_values('Pay', ascending=False)

    def max_fuel(self, aircraft):
        return aircraft['Ext1'] + aircraft['LTip'] + aircraft['LAux'] + aircraft['LMain'] + aircraft['Center1'] \
               + aircraft['Center2'] + aircraft['Center3'] + aircraft['RMain'] + aircraft['RAux'] + aircraft['RTip'] \
               + aircraft['RExt2']

    def estimated_fuel(self, distance, aircraft):
        # Add 1.5 hours
        fuel_weight = 0.81 if aircraft['FuelType'] == 1 else 0.721
        return (((round(distance / aircraft['CruiseSpeed'], 1)) * aircraft['GPH']) + (aircraft['GPH'] * 1.5)) * fuel_weight

    def get_best_assignments(self, row):
        max_cargo = 0
        if row['UnitType'] == 'passengers':
            df = self.assignments[(self.assignments.FromIcao == row['FromIcao']) &
                                  (self.assignments.ToIcao == row['ToIcao']) &
                                  (self.assignments.Amount <= row['Seats']) &
                                  (self.assignments.UnitType == 'passengers')]
        else:
            distance = self.get_distance(row['FromIcao'], row['ToIcao'])

            max_cargo = round(row['aircraft']['MTOW'] - row['aircraft']['EmptyWeight'] - self.estimated_fuel(distance, row['aircraft']))

            if max_cargo <= 0:
                return None

            df = self.assignments[(self.assignments.FromIcao == row['FromIcao']) &
                                  (self.assignments.ToIcao == row['ToIcao']) &
                                  (self.assignments.Amount <= max_cargo) &
                                  (self.assignments.UnitType == 'kg')]

        if not len(df):
            return None
        prob = LpProblem("Knapsack problem", LpMaximize)
        w_list = df.Amount.tolist()
        p_list = df.Pay.tolist()
        x_list = [LpVariable('x{}'.format(i), 0, 1, 'Integer') for i in range(1, 1 + len(w_list))]
        prob += sum([x * p for x, p in zip(x_list, p_list)]), 'obj'
        if row['UnitType'] == 'passengers':
            prob += sum([x * w for x, w in zip(x_list, w_list)]) <= row['Seats'], 'c1'
        else:
            prob += sum([x * w for x, w in zip(x_list, w_list)]) <= row['aircraft']['MTOW'], 'c1'
        prob.solve()
        return df.iloc[[i for i in range(len(x_list)) if x_list[i].varValue]]

    def get_aircraft_by_icao(self, icao):
        data = common.retry(self.get_query, const.LINK + 'query=icao&search=aircraft&icao={}'.format(icao),
                            error_type=TooManyConnectionsException)
        aircrafts = pd.DataFrame.from_csv(StringIO(data))
        aircrafts.RentalDry = aircrafts.RentalDry.astype(float)
        aircrafts.RentalWet = aircrafts.RentalWet.astype(float)
        return aircrafts

    def get_best_craft(self, icao, radius):
        print('Searching for the best aircraft from {}'.format(icao))
        max_mtow = 0
        best_aircraft = None
        for near_icao in self.get_closest_airports(icao, radius).icao:
            print('--Searching for the best aircraft from {}'.format(near_icao))
            aircraft = self.get_aircraft_by_icao(near_icao)
            if not len(aircraft):
                continue
            merged = pd.DataFrame.merge(aircraft, self.aircraft, left_on='MakeModel', right_on='Model', how='inner')
            merged = merged[
                (~merged.MakeModel.isin(const.IGNORED_AIRCRAFTS)) & (merged.RentalWet + merged.RentalDry > 0)]
            if not len(merged):
                continue
            aircraft = merged.ix[merged.Seats.idxmax()]
            if aircraft.MTOW > max_mtow:
                best_aircraft = aircraft
                max_mtow = aircraft.MTOW
        return best_aircraft

    def get_closest_airports(self, icao, nm):
        lat = self.airports[self.airports.icao == icao].lat.iloc[0]
        nm = float(nm)
        # one degree of latitude is appr. 69 nm
        lat_min = lat - nm / 69
        lat_max = lat + nm / 69
        filtered_airports = self.airports[self.airports.lat > lat_min]
        filtered_airports = filtered_airports[filtered_airports.lat < lat_max]
        distance_vector = filtered_airports.icao.map(lambda x: self.get_distance(icao, x))
        return filtered_airports[distance_vector < nm]

    def get_distance(self, from_icao, to_icao):
        lat1, lon1 = [radians(x) for x in self.airports[self.airports.icao == from_icao][['lat', 'lon']].iloc[0]]
        lat2, lon2 = [radians(x) for x in self.airports[self.airports.icao == to_icao][['lat', 'lon']].iloc[0]]
        return common.get_distance(lat1, lon1, lat2, lon2)

    def get_jobs_from(self, icaos):
        return common.retry(self.get_query, const.LINK + 'query=icao&search=jobsfrom&icaos={}'.format('-'.join(icaos)),
                            error_type=TooManyConnectionsException)

    def get_query(self, query_link):
        if self.service_key:
            query_link += '&servicekey={}'.format(self.service_key)
        while time.time() - self.last_request_time < 2.5:
            time.sleep(1)
        resource = urlopen(query_link)
        result = resource.read().decode(resource.headers.get_content_charset())
        self.last_request_time = time.time()
        if 'many requests in 60 second period' in result:
            print('Too many requests error. Use sevice key to avoid this error.')
            raise TooManyConnectionsException(result)
        if 'request was under the minimum delay' in result:
            raise TooManyConnectionsException(result)
        if 'Currently Closed for Maintenance' in result:
            raise ServerUnreachableException(result)
        return result
