import csv
import json
import aiohttp
from datetime import datetime
from django.http import HttpResponse
from django.shortcuts import render
from django.views import View
from django.core.files.storage import default_storage

class Candle:
    def __init__(self, symbol, datetime, open, high, low, close, volume):
        self.symbol = symbol
        self.datetime = datetime
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
    
    def to_dict(self):
        return {
            'symbol': self.symbol,
            'datetime': self.datetime.strftime('%Y%m%d %H:%M'),
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume
        }

class UploadCSVFileView(View):
    async def get(self, request):
        return render(request, 'upload_csv_file.html')

    async def fetch_csv_url(self, csv_file_url):
        async with aiohttp.ClientSession() as session:
            async with session.get(csv_file_url) as response:
                if response.status == 200:
                    return await response.read()
                else:
                    raise Exception(f"Failed to fetch CSV file, status code: {response.status}")
                
    async def post(self, request):
        csv_file_url = "https://drive.google.com/uc?id=19KnvzgABqMPYBrQF2-qxU1hfiAOkc9DA"
        timeframe = int(request.POST['timeframe'])

        try:
            csv_content = await self.fetch_csv_url(csv_file_url)
            file_name = 'uploaded_data.csv'
            file_path = default_storage.path(file_name)
            
            with open(file_path, 'wb') as f:
                f.write(csv_content)

            candles = await async_process_csv_file(file_path)
            
            # Convert objects to dictionary
            converted_candles = [candle.to_dict() for candle in candles]

            output_file = 'converted_data.json'
            output_path = default_storage.path(output_file)
            save_to_json(converted_candles, output_path)

            with open(output_path, 'rb') as f:
                response = HttpResponse(f.read(), content_type='application/json')
                response['Content-Disposition'] = f'attachment; filename={output_file}'
                return response
        except Exception as e:
            return HttpResponse(f"Failed to process CSV file: {e}", status=500)


async def async_process_csv_file(file_path):
    candles = []
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            try:
                symbol = row.get('BANKNIFTY')
                date_str = row.get('DATE')
                time_str = row.get('TIME')
                datetime_str = f"{date_str} {time_str}"
                try:
                    datetime_obj = datetime.strptime(datetime_str, '%Y%m%d %H:%M')
                except ValueError as e:
                    print(f"Error in row {i}: {e}")
                    continue

                volume_str = row.get('VOLUME', '').strip().replace('-', '')
                volume = int(volume_str) if volume_str.isdigit() else 0

                candle = Candle(
                    symbol=symbol,
                    datetime=datetime_obj,
                    open=float(row.get('OPEN', 0.0)),
                    high=float(row.get('HIGH', 0.0)),
                    low=float(row.get('LOW', 0.0)),
                    close=float(row.get('CLOSE', 0.0)),
                    volume=volume
                )
                candles.append(candle)
            except AttributeError as e:
                print(f"Unexpected error in row {i}: 'NoneType' object has no attribute: {e}")
            except Exception as e:
                print(f"Unexpected error in row {i}: {e}")
    return candles

def save_to_json(data, output_path):
    with open(output_path, 'w') as f:
        json.dump(data, f, default=str, indent=4)
