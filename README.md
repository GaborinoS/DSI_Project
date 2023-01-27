# DSI_Project

Please note that the file "PW.txt" is missing from the folders and is required for access to the PostgreSQL database

# Program Flow Chart

![Alt text](/readme/diagram.png?raw=true "Program Flow Chart")

## Web Scraping from Wiki
wiki/Web_Scraper.ipynb

The file "wiki/Web_Scraper.ipynb" utilizes the BeautifulSoup library to scrape information on S&P 500 companies from the Wikipedia website (https://en.wikipedia.org/wiki/List_of_S%26P_500_companies) and subsequent company-specific pages, and stores the infobox data in a PostgreSQL database.

## Yahoo Finance API
kafka-steam/kafka_python-app/producer.py

The file "kafka-stream/kafka_python-app/producer.py" utilizes the symbols scraped from Wikipedia to retrieve stock data from the Yahoo Finance API, which is then streamed to Kafka.

## InfluxDB
kafka-steam/kafka_python-app/consumer.py

The file "kafka-stream/kafka_python-app/consumer.py" streams the data from Kafka to InfluxDB.

## Dashboard
Dashboard/streamlit_finance.py

The Streamlit dashboard displays data from both InfluxDB and PostgreSQL. The dashboard retrieves all data from PostgreSQL only once and stores it in a dataframe for efficient access. In contrast, every time a user selects a company via the dropdown menu, the dashboard retrieves the most recent stock data from InfluxDB and dynamically displays it in a Plotly graph, ensuring that the data is always up-to-date.
