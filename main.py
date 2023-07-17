import requests
import urllib.parse
import pandas as pd

def address_to_fips(address: str) -> dict:

    print(f'Getting FIPS code for {address}')

    # Encode address to lat lon
    url = 'https://nominatim.openstreetmap.org/search/' + urllib.parse.quote(address) + '?format=json'
    response = requests.get(url).json()

    # Get lat and lon from response
    lat = response[0]["lat"]
    lon = response[0]["lon"]

    # Encode parameters for FIPS code
    params = urllib.parse.urlencode({'latitude': lat, 'longitude': lon, 'format': 'json'})

    # Contruct request URL
    url = 'https://geo.fcc.gov/api/census/block/find?' + params

    # Get response from FIPS API
    response = requests.get(url)

    # Parse json in response
    data = response.json()

    return data

# Remove last 3 digits from FIPS code
# address = '2716 Indiana Ave, St. Louis, MO 63118'
# data = address_to_fips(address)
# fips_for_neighborhood_atlas = data['Block']['FIPS'][:-3]

# Read in addresses
input_addresses = pd.read_csv('addresses_ra.csv')
input_addresses['nat_rank'] = -1

# Read in ADI data
adi_data = pd.read_csv('US_2020_ADI_Census Block Group_v3.2.csv')

# Lookup FIPS code in neighborhood atlas


# address = input_addresses.iloc[1]['address']
for i, row in input_addresses.iterrows():

    address = row['address']
    address = address.lower()

    try:
        data = address_to_fips(address=address)
        fips_for_neighborhood_atlas = data['Block']['FIPS'][:-3]

        nat_rank = adi_data[adi_data['FIPS'] == int(fips_for_neighborhood_atlas)]['ADI_NATRANK'].values[0]

        input_addresses.at[i, 'nat_rank'] = nat_rank

        print(f'Entry: {i} | Address: {address} | National ADI Rank: {nat_rank}')

    except:
        print(f'Could not process Entry: {i} | Address: {address} ')

    print('*' * 50)


input_addresses.to_csv('nat_ranks.csv', index=False)