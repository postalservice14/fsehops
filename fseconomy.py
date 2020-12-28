from urllib.parse import quote
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
    def __init__(self, local, service_key=None, user_key=None):
        self.last_request_time = time.time()
        self.service_key = service_key
        self.user_key = user_key

        self.all_airports = common.load_airports()
        self.aircraft = common.load_aircraft()

        if local:
            self.allowed_aircraft_airports = common.load_pickled_allowed_aircraft_airports()
            self.airports = self.get_airports()
            self.assignments = common.load_pickled_assignments()
        else:
            self.allowed_aircraft_airports = self.get_allowed_aircraft_airports()
            self.airports = self.get_airports()
            self.assignments = self.get_assignments()

    def get_airports(self):
        return self.all_airports[self.all_airports.icao.isin(self.allowed_aircraft_airports.Location)]

    def get_allowed_aircraft_airports(self):
        allowed_aircraft_airports = pd.DataFrame()
        for allowed_aircraft in const.ALLOWED_AIRCRAFTS:
            data = StringIO(self.get_airports_for(allowed_aircraft))
            allowed_aircraft_airports = pd.concat(
                [allowed_aircraft_airports, pd.read_csv(data, index_col=0, parse_dates=True)])

        with open('airports', 'wb') as f:
            pickle.dump(allowed_aircraft_airports, f)

        return allowed_aircraft_airports

    def get_assignments(self):
        assignments = pd.DataFrame()

        i = 0
        lim = len(self.airports)
        while i + 1500 < lim:
            data = StringIO(self.get_jobs_from(self.airports.icao[i:i + 1500]))
            assignments = pd.concat([assignments, pd.read_csv(data, index_col=0, parse_dates=True)])
            i += 1500
            print(i)
        data = StringIO(self.get_jobs_from(self.airports.icao[i:lim - 1]))
        assignments = pd.concat([assignments, pd.read_csv(data, index_col=0, parse_dates=True)])
        with open('assignments', 'wb') as f:
            pickle.dump(assignments, f)
        return assignments

    def get_aggregated_assignments(self):
        grouped = self.assignments.groupby(['FromIcao', 'ToIcao', 'UnitType'], as_index=False)
        aggregated = grouped.aggregate(np.sum)
        return aggregated.sort_values('Pay', ascending=False)

    def get_best_assignments(self, row):
        df = self.assignments[((self.assignments.FromIcao == row['FromIcao']) &
                               (self.assignments.ToIcao == row['ToIcao']) &
                               (self.assignments.Type != 'VIP') &
                               (self.assignments.Amount <= row['MaxPassengers']) &
                               (self.assignments.UnitType == 'passengers')) |
                              ((self.assignments.FromIcao == row['FromIcao']) &
                               (self.assignments.ToIcao == row['ToIcao']) &
                               (self.assignments.Type != 'VIP') &
                               (self.assignments.Amount <= row['MaxCargo']) &
                               (self.assignments.UnitType == 'kg'))]

        if not len(df):
            return None

        dfd = df.copy()
        mask = dfd['UnitType'].str.match('passengers')

        dfd.loc[mask, 'Passengers'] = dfd['Amount']
        dfd['Passengers'] = dfd['Passengers'].fillna(0)
        dfd.loc[mask, 'Amount'] *= const.PAX_WEIGHT_KG

        prob = LpProblem("KnapsackProblem", LpMaximize)
        weight_list = dfd.Amount.tolist()
        pax_list = dfd.Passengers.tolist()
        pay_list = dfd.Pay.tolist()
        x_list = [LpVariable('x{}'.format(i), 0, 1, 'Integer') for i in range(1, 1 + len(weight_list))]
        prob += sum([x * p for x, p in zip(x_list, pay_list)]), 'obj'
        prob += sum([x * w for x, w in zip(x_list, pax_list)]) <= row['MaxPassengers'], 'c1'
        prob += sum([x * w for x, w in zip(x_list, weight_list)]) <= row['MaxCargo'], 'c2'
        prob.solve()
        best_assignments = dfd.iloc[[i for i in range(len(x_list)) if x_list[i].varValue]]

        best_vip_assignment = self.get_best_vip_assignment(row)

        if best_vip_assignment is not None and (sum(best_vip_assignment['Pay']) >= sum(best_assignments['Pay'])):
            return best_vip_assignment

        return best_assignments

    def get_best_vip_assignment(self, row):
        df = self.assignments[((self.assignments.FromIcao == row['FromIcao']) &
                               (self.assignments.ToIcao == row['ToIcao']) &
                               (self.assignments.Type == 'VIP') &
                               (self.assignments.Amount <= row['MaxPassengers']) &
                               (self.assignments.UnitType == 'passengers')) |
                              ((self.assignments.FromIcao == row['FromIcao']) &
                               (self.assignments.ToIcao == row['ToIcao']) &
                               (self.assignments.Type == 'VIP') &
                               (self.assignments.Amount <= row['MaxCargo']) &
                               (self.assignments.UnitType == 'kg'))]

        if not len(df):
            return None

        return df.sort_values('Pay', ascending=False).head(1)

    def get_aircraft_by_icao(self, icao):
        aircrafts = self.allowed_aircraft_airports[self.allowed_aircraft_airports.Location == icao]
        if 'RentalDry' not in aircrafts.columns or 'RentalWet' not in aircrafts.columns:
            return []

        # aircrafts.RentalDry = aircrafts.RentalDry.astype(float)
        # aircrafts.RentalWet = aircrafts.RentalWet.astype(float)
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
                merged.MakeModel.isin(const.ALLOWED_AIRCRAFTS) & (merged.RentalWet + merged.RentalDry > 0)]
            if not len(merged):
                continue
            aircraft = merged.loc[merged.Seats.idxmax()]
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
        lat1, lon1 = [radians(x) for x in
                      self.all_airports[self.all_airports.icao == from_icao][['lat', 'lon']].iloc[0]]
        lat2, lon2 = [radians(x) for x in self.all_airports[self.all_airports.icao == to_icao][['lat', 'lon']].iloc[0]]
        return common.get_distance(lat1, lon1, lat2, lon2)

    def get_jobs_from(self, icaos):
        return common.retry(self.get_query, const.LINK + 'query=icao&search=jobsfrom&icaos={}'.format('-'.join(icaos)),
                            error_type=TooManyConnectionsException)

    def get_airports_for(self, makeModel):
        print('Searching for the airports with aircraft {}'.format(makeModel))
        return common.retry(self.get_query,
                            const.LINK + 'query=aircraft&search=makemodel&makemodel={}'.format(
                                quote(makeModel.encode("utf-8"))),
                            error_type=TooManyConnectionsException)

    def get_query(self, query_link):
        if self.service_key:
            query_link += '&servicekey={}'.format(self.service_key)
        elif self.user_key:
            query_link += '&userkey={}'.format(self.user_key)
        while time.time() - self.last_request_time < 6:
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
