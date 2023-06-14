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
        - Create aggregations in databricks for rolling averages
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
    - Without DAX -->
        - As requested by client all aggregations and data transformations were created within databricks and dataset with rolling averages was loaded to presentation layer of azure data lake storage
        - Rolling averages dataset was imported to power bi desktop using azure databricks connector and line chart was created as follows:
        
        ![Alt text](/images/rolling_avg.png "Power BI Service Report")
        - Check following link to access above report:
        https://app.powerbi.com/groups/3325ad30-c355-4373-9bda-3596b5dad2b4/reports/1fe35262-96a0-4e37-b340-aca9daa2b06d/ReportSection?experience=power-bi

    - With DAX --> 
        - Create a data model with three tables --> symbol_dim, date_dim, coin_metrics_fact (IN PROGRESS)
        - Visualize rolling averages using DAX measures (IN PROGRESS)

- CI/CD Devops
    - This code base is housed in Azure Devops Repos 
    - Devlopment work is done in feature/meet_dev branch
    - Pull Request is made to main branch
    - Once approved and merged to main branch both build and release pipelines are triggered sequentially
        - Build pipeline (check build-notebooks.yml in project root) builds artifact which comprises of python and sql notebooks in codebase. Once complete it triggers release pipeline
        
        ![Alt text](/images/build_pipeline.png "Build Pipeline Run")
        - Artifacts are funnelled to release pipeline

        ![Alt text](/images/artifact.png "Build Artifacts")
        - Release pipeline (check release-notebooks.yml in project root) releases built artifact to databricks workspace using Databricks CLI
        
        ![Alt text](/images/release_pipeline.png "Release Pipeline Run")
        - Newly released notebooks can be accessed by any developer working on databricks workspace with appropriate permissions
        
        ![Alt text](/images/databricks_release.png "Release in DB Workspace")