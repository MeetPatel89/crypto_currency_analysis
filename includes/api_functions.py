# Databricks notebook source
# function to make calls to cryptocompare API
def get_data(url, parameters, headers):
    session = Session()
    session.headers.update(headers)
    try:
        response = session.get(url, params=parameters)
        data = json.loads(response.text)
        return data
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)

# COMMAND ----------

# function to get api data for top 10 coins
def get_coin_data(coin_list, url, headers):
    coin_data = []
    parameters = { 
        "tsym":"USD",
        "allData":"true"
    }
    for coin in coin_list:
        parameters["fsym"] = coin
        res_json = get_data(url, parameters, headers)
        data = res_json["Data"]
        # iterate through the data and add the coin name to each row
        for row in data:
            row["symbol"] = coin
        coin_data.extend(data)
    return coin_data
