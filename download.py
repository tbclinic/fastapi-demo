from zipfile import ZipFile
import requests

db_path = './postalcode.db'
file_name = "postalcode"

def download_zip(link, parameters):
    response = requests.get(link, params=parameters)
    with open(f"{file_name}.zip", mode="wb") as file:
        file.write(response.content)
        file.close()
    with ZipFile(f"{file_name}.zip", 'r') as zip:
        zipinfo = zip.infolist()
        for item in zipinfo:
            if item.filename.endswith('.csv'):
                csv_file_name = item.filename
                break

        if csv_file_name is not None:
            print('Extracting all the files now...')
            zip.extractall()  # Extracting all files
            print('Done!')
        else:
            print("No CSV file found in the ZIP.")
    return csv_file_name
