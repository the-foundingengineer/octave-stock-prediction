
# from certifi import contents
from google import genai
from google.genai import types

import os

GEMINI_API_KEY = 'AIzaSyDFCx8XOLxmN3zVZfBwQeoizGQf-9QdK58'

client = genai.Client(api_key=GEMINI_API_KEY)

def call_gemini(prompt: str) -> str:
    """
    Sends a natural language prompt to Gemini and returns the text response.
    """
    response = client.models.generate_content(
        model="gemini-2.5-flash",  # Choose Flash, Pro, etc.
        contents=prompt,
        config=types.GenerateContentConfig(
        temperature=0,
        top_p=0.95,
        top_k=20,
    ),
        # temperature=0.2  # Lower for factual explanations
    )
    
    # Gemini returns an array of candidates — take the first text
    return response.text