
# from certifi import contents
from google import genai
from google.genai import types

import os

# client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# def call_gemini(prompt: str) -> str:
#     """
#     Sends a natural language prompt to Gemini and returns the text response.
#     """
#     response = client.models.generate_content(
#         model="gemini-2.5-flash",  # Choose Flash, Pro, etc.
#         contents=prompt,
#         config=types.GenerateContentConfig(
#         temperature=0,
#         top_p=0.95,
#         top_k=20,
#     ),
#         # temperature=0.2  # Lower for factual explanations
#     )
    
#     # Gemini returns an array of candidates — take the first text
#     return response.text

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# model = genai.GenerativeModel("models/gemini-2.5-flash")


SYSTEM_PROMPT = """
You are a financial AI assistant specialized strictly in Nigerian stocks listed on the Nigerian Exchange (NGX).

Rules:
1. Only answer questions related to Nigerian-listed companies.
2. If the user asks about a non-Nigerian stock, respond politely with:
   "This AI assistant only provides analysis for Nigerian stocks. Please ask about a Nigerian-listed company."
3. Verify by yourself if the stock mentioned is Nigerian or not.
4. Provide professional, structured financial analysis.
5. Clearly state assumptions when forecasting.
6. Do not guarantee investment outcomes.
"""


def generate_ai_response(question: str) -> str:
    full_prompt = f"{SYSTEM_PROMPT}\n\nUser Question:\n{question}"
    response = client.models.generate_content(
        model="gemini-2.5-flash",  # Choose Flash, Pro, etc.
        contents=full_prompt,
        config=types.GenerateContentConfig(
        temperature=0,
        top_p=0.95,
        top_k=20,
        )
    )
    return response.text