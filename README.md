# Top 10 CryptoCurrencies Analysis

## Introduction 
This is an exploratory data analysis and visualization project as per guidelines and requests by client that is looking to assess feasibility of migrating their data from on premises to azure cloud using data lakehouse architecture native to databricks

## Client Guidelines
- Make api request to 'https://min-api.cryptocompare.com' and get data for top 10 currencies by market cap
- Exploratory data analysis asks
    - HV Ratio for each coin
    - Mean Closing price for each coin 
    - Max and Min for volumes
    - Max metrics (open, high, close, low)
    - Average monthly closing price
- Architectural guidelines
    - Entire project should be within azure end-to-end
    - Use azure databricks for data analytics
    - Notebooks job should be run on a weekly schedule
- Visualization (Two reports)
    - Without DAX -->
        - Create aggregations in databricks for rolling averages over 5, 7, 10, 15, 30 days
        - Use power bi desktop and service to visualize rolling averages
    - Wtih DAX -->
        - Visualize rolling averages in power bi desktop and service using dax 
        - We are checking accessibility of power bi for our business team

## Delivery
- Notebooks are divided into three folders:
    - data-integration --> Make API calls to get daily history data for top 10 coins and storing it in raw container (ADLS Gen2)
    - data-modelling --> Model raw data into three tables - coin_metrics_fact, date_dim, symbol_dim - store them in processed container (ADLS Gen2)
    - data-presentation --> Do exploratory analysis and extract metrics for visualization - store data in presentation container (ADLS Gen2)
- Jobs
    - All notebooks for data integration, data modelling and data presentation are running in a databricks job cluster schedule to run every Sunday
    - Please reach out using contact details mentioned in resume if you want notifications for the job run output
- Visualization
    - TODO
