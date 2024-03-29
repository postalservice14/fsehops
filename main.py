import argparse
import pandas as pd

# import modin.pandas as pd

import const
from fseconomy import FSEconomy
import common
from math import floor


def do_work(args):
    fse = FSEconomy(args.local, args.skey, args.ukey)

    for col in fse.assignments.columns:
        if 'Unnamed' in col:
            del fse.assignments[col]

    aggregated = fse.get_aggregated_assignments()

    result = pd.DataFrame(
        columns=['FromIcao', 'ToIcao', 'Amount', 'Pay', 'Assignments', 'MakeModel', 'Registration', 'Location', 'Seats',
                 'MTOW',
                 'CruiseSpeed', 'RentalDry', 'RentalWet', 'CraftDistance', 'Distance', 'PayloadNow',
                 'AircraftFuelForTripGal', 'MaxCargo', 'MaxPassengers', 'DryRent', 'WetRent', 'DryEarnings',
                 'WetEarnings', 'DryRatio', 'WetRatio'])

    # new_aggregated = aggregated[aggregated['FromIcao'].str.startswith('K')]
    # new_aggregated = new_aggregated.append(aggregated[aggregated['FromIcao'].str.startswith('E')])
    new_aggregated = aggregated.sort_values('Pay', ascending=False)

    # new_aggregated = aggregated.sort_values('Pay', ascending=False)

    index = 0
    for _, row in new_aggregated.iterrows():
        if index >= args.limit:
            break
        best_aircrafts = fse.get_close_aircraft(row['FromIcao'], args.radius)

        if best_aircrafts is None:
            break

        for best_aircraft in best_aircrafts:
            if best_aircraft is None:
                continue
            for column in ['MakeModel', 'Registration', 'Location', 'Seats', 'MTOW', 'CruiseSpeed', 'RentalDry',
                           'RentalWet']:
                row[column] = best_aircraft[column]
            row['aircraft'] = best_aircraft
            row['Distance'] = fse.get_distance(row['FromIcao'], row['ToIcao'])

            additional_crew = row['aircraft']['Crew']
            fuel_cap = common.get_max_fuel(row['aircraft'])
            payload = int(row['aircraft']['MTOW']) - int(row['aircraft']['EmptyWeight']) - (
                    const.PAX_WEIGHT_KG * (1 + additional_crew))
            payload75 = round(payload - fuel_cap * const.GALLONS_TO_KG)
            payload100 = round(payload - fuel_cap * const.GALLONS_TO_KG)
            payloadnow = round(payload - common.get_total_fuel(row['aircraft']) * const.GALLONS_TO_KG)
            row['PayloadNow'] = payloadnow
            crewseats = 1
            if additional_crew > 0:
                crewseats = 2

            seats = row['aircraft']['Seats'] - crewseats

            estimated_fuel_needed = common.get_estimated_fuel_needed(row['Distance'], row['aircraft'])
            if estimated_fuel_needed > fuel_cap:
                continue
            hours_needed = estimated_fuel_needed / row['aircraft']['GPH']
            seconds_needed = hours_needed * 60 * 60
            if row['aircraft']['RentalTime'] < seconds_needed:
                continue

            row['AircraftFuelForTripGal'] = max(common.get_estimated_fuel_needed(row['Distance'], row['aircraft']),
                                                common.get_total_fuel(row['aircraft']))

            percent = min(row['AircraftFuelForTripGal'] / fuel_cap * 100, 100)
            max_payload = floor((100 - percent) * ((payload75 - payload100) / 25) + payload100)
            max_pax = min(seats, floor(max_payload / const.PAX_WEIGHT_KG))

            row['MaxCargo'] = max_payload
            row['MaxPassengers'] = max_pax
            best_assignments = fse.get_best_assignments(row)
            if best_assignments is None:
                continue
            row['Amount'] = sum(best_assignments['Amount'])
            row['Pay'] = sum(best_assignments['Pay'])
            row['Assignments'] = str(best_assignments['Amount'].tolist())
            row['CraftDistance'] = fse.get_distance(row['FromIcao'], row['aircraft']['Location'])
            row['DryRent'] = round(
                (row['Distance'] + row['CraftDistance']) * row['aircraft']['RentalDry'] / row['aircraft'][
                    'CruiseSpeed'], 2)
            row['WetRent'] = round(
                (row['Distance'] + row['CraftDistance']) * row['aircraft']['RentalWet'] / row['aircraft'][
                    'CruiseSpeed'], 2)
            row['DryEarnings'] = common.get_earnings(row, 'DryRent')
            row['WetEarnings'] = common.get_earnings(row, 'WetRent')
            if not row['DryEarnings'] + row['WetEarnings']:
                continue
            if row['DryEarnings'] < args.min and row['WetEarnings'] < args.min:
                continue

            row['DryRatio'] = common.get_ratio(row, 'DryEarnings')
            row['WetRatio'] = common.get_ratio(row, 'WetEarnings')
            result.loc[index] = row
            index += 1

    print(result.sort_values('DryEarnings', ascending=False).to_string())
    if args.debug:
        import pdb
        pdb.set_trace()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--skey', help='Service key')
    parser.add_argument('--ukey', help='User key')
    parser.add_argument('--limit', help='Limit for search', type=int, default=20)
    parser.add_argument('--radius', help='Radius for aircraft search (nm)', type=int, default=50)
    parser.add_argument('--local', help='Use local dump of assignments instead of update', action='store_true')
    parser.add_argument('--debug', help='Use this key to enable debug breakpoints', action='store_true')
    parser.add_argument('--min', help='Minimum earnings (time consuming)', type=int, default=1000)
    args = parser.parse_args()
    if not (args.skey or args.ukey):
        raise Exception('You have to provide userkey or service key')

    do_work(args)


if __name__ == '__main__':
    main()
