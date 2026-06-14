#########!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! API Interference Scripts !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!#############
# ~~~~Learning : Im out of API ,bcz i tried whole architecture with real api; every chnage and trial makes my api exhaustion;
# I could simulate the label and score to check to prevent exhaustion and then when works i oculd use my api ; so for testing alwas use dummy insted of fresh api to prevent resources

# import requests
import random
import re 
import asyncio
# import httpx
# # This is a public model for sentiment
# API_URL = "https://router.huggingface.co/hf-inference/models/cardiffnlp/twitter-roberta-base-sentiment-latest"
# headers = {"Authorization": "HF_token"}
# async_client = httpx.AsyncClient()

def clean_text(text):
    text = text.lower()
    text = re.sub(r'http\S+|www\S+|https\S+', '',text,flags=re.MULTILINE)
    text = re.sub(r'[^\w\s]', '',text)
    text = text.strip() 
    if(len(text) < 5):
        return None
    else:
        return text 
    
async def query_sentiment(text):
    text = clean_text(text)
    if text is None:
        return None, None
    await asyncio.sleep(0.05) 
    
    labels = ["positive", "negative", "neutral"]
    fake_label = random.choice(labels)
    fake_score = round(random.uniform(0.5, 0.99), 4)
    
    return fake_label, fake_score
    
# async def query_sentiment(text):
#     text = clean_text(text)
#     if not text: # Handles None or empty strings after cleaning
#         return None, None
    
#     payload = {"inputs": text}
    
#     for attempt in range(5):  # Increased to 5 attempts to account for model loading wait times
#         try:
#             response = await async_client.post(API_URL, headers=headers, json=payload, timeout=20.0)
            
#             # Case 1: Success
#             if response.status_code == 200:
#                 json_data = response.json()
                
#                 # Dynamic parsing to prevent indexing crashes
#                 if isinstance(json_data, list) and len(json_data) > 0:
#                     first_element = json_data[0]
#                     if isinstance(first_element, list) and len(first_element) > 0:
#                         data = first_element[0] # Handles [[{label, score}]]
#                     else:
#                         data = first_element    # Handles [{label, score}]
                    
#                     return data['label'], data['score']
            
#             # Case 2: Model is loading (Common on Hugging Face free tier)
#             elif response.status_code == 503:
#                 error_data = response.json()
#                 wait_time = error_data.get("estimated_time", 5.0)
#                 print(f"⚠️ Model loading. Waiting {wait_time}s (Attempt {attempt+1}/5)...")
#                 await asyncio.sleep(wait_time)
#                 continue
                
#             # Case 3: Rate Limited
#             elif response.status_code == 429:
#                 sleep_duration = 2 ** attempt
#                 print(f"⚠️ Rate limited (429). Backing off for {sleep_duration}s...")
#                 await asyncio.sleep(sleep_duration)
#                 continue
                
#             # Case 4: Any other unexpected status code (400, 401, 402, 404, 500)
#             else:
#                 print(f"❌ HF API Error Code {response.status_code}: {response.text}")
#                 await asyncio.sleep(1)
                
#         except Exception as e:
#             print(f"💥 Network/Parsing Exception on attempt {attempt+1}: {str(e)}")
#             await asyncio.sleep(1)       
#     return None, None

# --------------------------------------------------------------------------------------------------------------------
# data = "Zomato delivery was 20 minutes late and the food was cold. Very disappointed."
# label,score = query_sentiment(data)
# print(f"Label : {label} Score : {score}")
