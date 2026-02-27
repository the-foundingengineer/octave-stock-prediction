from app.ai.gemini_service import generate_ai_response

def process_ai_question(question: str):

    # 2️⃣ Generate AI response
    ai_text = generate_ai_response(question)

    return {
        "answer": ai_text,
        "disclaimer": "This response is AI-generated and does not constitute financial advice."
    }