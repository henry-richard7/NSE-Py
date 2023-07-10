import requests
from urllib.parse import quote
import pandas as pd
from io import StringIO

class NseAPI:
    def __init__(self) -> None:
        self.HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Edg/93.0.961.52",
        "X-Requested-With": "XMLHttpRequest"
        }

        self.client = requests.Session()
        self.client.get("https://www.nseindia.com/market-data/live-equity-market",headers=self.HEADERS)
        
    def get_indices(self):
        """
        Get the indices from the NSE India API.
        
        Returns:
            A list of indices.
        """
        response = self.client.get(
            "https://www.nseindia.com/api/equity-master",
            headers=self.HEADERS
        ).json()

        return [symbol for symbols in response.keys() for symbol in response[symbols]]
    
    def get_all_stocks(self, index: str):
        """
        Retrieves all stocks from the NSE equity-stockIndices API based on the given index.

        Args:
            index (str): The index for which to retrieve the stocks.

        Returns:
            list: A list of dictionaries containing the stock symbol and company name of each stock.
        """
        url = f"https://www.nseindia.com/api/equity-stockIndices?index={quote(index.upper())}"

        response = self.client.get(url, headers=self.HEADERS).json()['data']

        results = [
            {
                "stock_symbol": stock['symbol'],
                "company_name": stock.get('meta', {}).get('companyName')
            }
            for stock in response
            if stock['symbol'] != index.upper()
        ]
        
        return results

    def index_data(self, stock_index):
        """
        Indexes data for a given stock index.

        Args:
            stock_index (str): The stock index to index data for.

        Returns:
            List[Dict[str, Union[str, float]]]: A list of dictionaries containing the indexed data. Each dictionary contains the following keys:
                - symbol (str): The symbol of the stock.
                - open (float): The opening price of the stock.
                - dayHigh (float): The highest price of the stock in a day.
                - dayLow (float): The lowest price of the stock in a day.
                - lastPrice (float): The last traded price of the stock.
                - previousClose (float): The previous closing price of the stock.
                - change (float): The change in price of the stock.
                - pChange (float): The percentage change in price of the stock.
                - 52W_High (float): The 52-week high price of the stock.
                - 52W_Low (float): The 52-week low price of the stock.
                - totalTradedVolume (float): The total traded volume of the stock.
                - totalTradedValue (float): The total traded value of the stock.
                - perChange365d (float): The percentage change in price of the stock over the last 365 days.
                - perChange30d (float): The percentage change in price of the stock over the last 30 days.
                - last_updated_at (str): The timestamp when the data was last updated.

            None: If the response status code is not 200.
        """
        url = f"https://www.nseindia.com/api/equity-stockIndices?index={stock_index}"
        response = self.client.get(url, headers=self.HEADERS)

        if response.status_code != 200:
            return None
        
        j_response = response.json()
        last_updated = j_response['timestamp']
        result = []

        for i, datum in enumerate(j_response['data']):
            temp_dict = {
                'symbol': datum['symbol'],
                'open': datum['open'],
                'dayHigh': datum['dayHigh'],
                'dayLow': datum['dayLow'],
                'lastPrice': datum['lastPrice'],
                'previousClose': datum['previousClose'],
                'change': datum['change'],
                'pChange': datum['pChange'],
                '52W_High': datum['yearHigh'],
                '52W_Low': datum['yearLow'],
                'totalTradedVolume': datum['totalTradedVolume'],
                'totalTradedValue': datum['totalTradedValue'],
                'perChange365d': datum['perChange365d'],
                'perChange30d': datum['perChange30d'],
                'last_updated_at': last_updated
            }

            if i != 0:
                temp_dict['company_name'] = datum['meta'].get("companyName")

            result.append(temp_dict)

        return result


    def get_stock_data(self, symbol):
        """
        Retrieves stock data for a given symbol.

        Parameters:
            symbol (str): The symbol of the stock.

        Returns:
            dict: A dictionary containing various stock data including symbol, company name, industry,
                  listing date, open price, last price, previous close, high price, low price, change,
                  pChange, 52-week high date, 52-week high, 52-week low date, 52-week low, total traded volume,
                  total buy quantity, total sell quantity, and last update time.

                  Returns None if the stock data cannot be retrieved.
        """
        response = self.client.get(f"https://www.nseindia.com/api/quote-equity?symbol={symbol}", headers=self.HEADERS)

        if response.status_code == 200 and response.json().get("info"):
            result = response.json()
            final_dict = {
                'symbol': result['info']['symbol'],
                'company_name': result['info']['companyName'],
                'industry': result['info'].get('industry'),
                'listingDate': result['metadata']['listingDate'],
                'open_price': result['priceInfo']['open'],
                'last_price': result['priceInfo']['lastPrice'],
                'previous_close': result['priceInfo']['previousClose'],
                'high_price': result['priceInfo']['intraDayHighLow']['max'],
                'low_price': result['priceInfo']['intraDayHighLow']['min'],
                'change': round(result['priceInfo']['change'], 2),
                'pChange': round(result['priceInfo']['pChange'], 2),
                '52w_high_date': result['priceInfo']['weekHighLow']['maxDate'],
                '52w_high': result['priceInfo']['weekHighLow']['max'],
                '52w_low_date': result['priceInfo']['weekHighLow']['minDate'],
                '52w_low': result['priceInfo']['weekHighLow']['min'],
                'total_traded_volume': result['preOpenMarket']['totalTradedVolume'],
                'total_buy_quantity': result['preOpenMarket']['totalBuyQuantity'],
                'total_sell_quantity': result['preOpenMarket']['totalSellQuantity'],
                'last_update_time': result['metadata']['lastUpdateTime']
            }

            return final_dict

        return None

    def get_historical_data(self, symbol, start_date, end_date):
        """
        Retrieves historical data for a given symbol within a specified date range.

        Args:
            symbol (str): The symbol of the stock.
            start_date (str): The start date of the historical data in the format "YYYY-MM-DD".
            end_date (str): The end date of the historical data in the format "YYYY-MM-DD".

        Returns:
            pandas.DataFrame: The historical data as a pandas DataFrame. Columns include "DATE", "SYMBOL", "SERIES",
            "OPEN", "HIGH", "LOW", "PREV_CLOSE", "LTP", "CLOSE", "VWAP", "52W_H", "52W_L", "VOLUME", "VALUE", and
            "NO_OF_TRADES".

        Raises:
            Exception: If there is an error retrieving the data from the API.
        """
        url = f'https://www.nseindia.com/api/historical/cm/equity?symbol={quote(symbol.upper())}&series=["EQ"]&from={start_date}&to={end_date}&csv=true'
        response = self.client.get(url, headers=self.HEADERS, timeout=1000)

        if response.status_code == 200:
            try:
                df = pd.read_csv(StringIO(response.text), thousands=",")
                df.columns = [
                    "DATE",
                    "SERIES",
                    "OPEN",
                    "HIGH",
                    "LOW",
                    "PREV_CLOSE",
                    "LTP",
                    "CLOSE",
                    "VWAP",
                    "52W_H",
                    "52W_L",
                    "VOLUME",
                    "VALUE",
                    "NO_OF_TRADES",
                ]
                df.insert(0, "SYMBOL", symbol)
                df["DATE"] = pd.to_datetime(df["DATE"], format="%d-%b-%Y").dt.date
                df['VWAP'] = pd.to_numeric(df['VWAP'], errors='coerce')

                return df

            except:
                raise Exception(f"Error: {response.json().get('showMessage')}")

        else:
            print(response.text)
            return None
