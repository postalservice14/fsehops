import argparse
import pandas as pd

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
        columns=['FromIcao', 'ToIcao', 'Amount', 'Pay', 'Assignments', 'MakeModel', 'Location', 'Seats', 'MTOW',
                 'CruiseSpeed', 'RentalDry', 'RentalWet', 'CraftDistance', 'Distance', 'PayloadNow',
                 'AircraftFuelForTripGal', 'MaxCargo', 'MaxPassengers', 'DryRent', 'WetRent', 'DryEarnings',
                 'WetEarnings', 'DryRatio', 'WetRatio'])

    index = 0
    for _, row in aggregated.iterrows():
        if not args.min and index >= args.limit:
            break
        best_aircraft = fse.get_best_craft(row['FromIcao'], args.radius)
        if best_aircraft is None:
            continue
        for column in ['MakeModel', 'Location', 'Seats', 'MTOW', 'CruiseSpeed', 'RentalDry', 'RentalWet']:
            row[column] = best_aircraft[column]
        row['aircraft'] = best_aircraft
        row['Distance'] = fse.get_distance(row['FromIcao'], row['ToIcao'])

        additional_crew = row['aircraft']['Crew']
        fuel_cap = common.get_max_fuel(row['aircraft'])
        payload = row['aircraft']['MTOW'] - row['aircraft']['EmptyWeight'] - (
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
            (row['Distance'] + row['CraftDistance']) * row['aircraft']['RentalDry'] / row['aircraft']['CruiseSpeed'], 2)
        row['WetRent'] = round(
            (row['Distance'] + row['CraftDistance']) * row['aircraft']['RentalWet'] / row['aircraft']['CruiseSpeed'], 2)
        row['DryEarnings'] = common.get_earnings(row, 'DryRent')
        row['WetEarnings'] = common.get_earnings(row, 'WetRent')
        if not row['DryEarnings'] + row['WetEarnings']:
            continue
        row['DryRatio'] = common.get_ratio(row, 'DryEarnings')
        row['WetRatio'] = common.get_ratio(row, 'WetEarnings')
        result.loc[index] = row
        index += 1
        if not args.min:
            continue
        if row['DryEarnings'] > args.min or row['WetEarnings'] > args.min:
            break

    # aggregated = aggregated.dropna()

    print(result.sort_values('DryRatio', ascending=False).to_string())
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
    parser.add_argument('--min', help='Minimum earnings (time consuming)', type=int)
    args = parser.parse_args()
    if not (args.skey or args.ukey):
        raise Exception('You have to provide userkey or service key')

    do_work(args)


if __name__ == '__main__':
    main()
