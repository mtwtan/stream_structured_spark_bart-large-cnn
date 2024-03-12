import os
import requests
import boto3

data_dict = {
   "Amazon_Fashion": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/AMAZON_FASHION_5.json.gz",
   "All_Beauty": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/All_Beauty_5.json.gz",
   "Appliances": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Appliances_5.json.gz",
   "Arts_Crafts_and_Sewing": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Arts_Crafts_and_Sewing_5.json.gz",
   "Automotive": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Automotive_5.json.gz",
   "Books": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Books_5.json.gz",
   "CDs_and_Vinyl": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/CDs_and_Vinyl_5.json.gz",
   "Cell_Phones_and_Accessories": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Cell_Phones_and_Accessories_5.json.gz",
   "Clothing_Shoes_and_Jewelry": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Clothing_Shoes_and_Jewelry_5.json.gz",
   "Digital_Music": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Digital_Music_5.json.gz",
   "Electronics": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Electronics_5.json.gz",
   "Gift_Cards": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Gift_Cards_5.json.gz",
   "Grocery_and_Gourmet_Food": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Grocery_and_Gourmet_Food_5.json.gz",
   "Home_and_Kitchen": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Home_and_Kitchen_5.json.gz",
   "Industrial_and_Scientific": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Industrial_and_Scientific_5.json.gz",
   "Kindle_Store": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Kindle_Store_5.json.gz",
   "Luxury_Beauty": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Luxury_Beauty_5.json.gz",
   "Magazine_Subscriptions": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Magazine_Subscriptions_5.json.gz",
   "Movies_and_TV": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Movies_and_TV_5.json.gz",
   "Musical_Instruments": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Musical_Instruments_5.json.gz",
   "Office_Products": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Office_Products_5.json.gz",
   "Patio_Lawn_and_Garden": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Patio_Lawn_and_Garden_5.json.gz",
   "Pet_Supplies": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Pet_Supplies_5.json.gz",
   "Prime_Pantry": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Prime_Pantry_5.json.gz",
   "Software": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Software_5.json.gz",
   "Sports_and_Outdoors": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Sports_and_Outdoors_5.json.gz",
   "Tools_and_Home_Improvement": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Tools_and_Home_Improvement_5.json.gz",
   "Toys_and_Games": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Toys_and_Games_5.json.gz",
   "Video_Games": "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Video_Games_5.json.gz"
}

s3 = boto3.client('s3')
bucket_name = "amzn-customer-reviews-XXXXXXXXXXX"

for key in data_dict:
    print(key, data_dict[key])

    r = requests.get(data_dict[key],allow_redirects=True,verify=False,stream=True)
    os.makedirs(key)
    filename = key + '/' + key + '.json.gz'
    with open(filename,'wb') as f:
       for chunk in r.iter_content(chunk_size = 1024*1024):
           if chunk:
              f.write(chunk)
    print(filename + ' downloaded')

    s3_key = 'category=' + filename
    s3.upload_file(filename,bucket_name,s3_key)

    print(filename + 'uploaded to s3 - s3://' + bucket_name + '/' + s3_key)


    s3_key = 'category=' + filename
    s3.upload_file(filename,bucket_name,s3_key)
