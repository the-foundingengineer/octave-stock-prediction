def get_stock_analysis_structured(stock_data):
    """
    Get structured analysis using function calling
    """
    tools = [{
        "type": "function",
        "function": {
            "name": "analyze_stock",
            "description": "Analyze stock OHLC data and provide structured insights",
            "parameters": {
                "type": "object",
                "properties": {
                    "trend": {
                        "type": "string",
                        "enum": ["bullish", "bearish", "neutral"],
                        "description": "Overall trend based on OHLC data"
                    },
                    "volatility": {
                        "type": "string",
                        "enum": ["high", "medium", "low"],
                        "description": "Price volatility assessment"
                    },
                    "price_prediction": {
                        "type": "number",
                         "enum": ["strong buy", "buy", "neutral", "sell", "strong sell"],
                        "description": "Buy or sell signal based on OHLC data"
                    },
                    "key_observations": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Key technical observations"
                    }
                },
                "required": ["trend", "volatility", "price_movement", "key_observations"]
            }
        }
    }]
    
    response = client.chat.responses.create(
        model="gpt-4",
        messages=[{
            "role": "user",
            "content": f"Analyze: Open=${stock_data['open']}, High=${stock_data['high']}, "
                      f"Low=${stock_data['low']}, Close=${stock_data['close']}"
        }],
        tools=tools,
        tool_choice={"type": "function", "function": {"name": "analyze_stock"}}
    )
    
    # Parse the function call response
    function_call = response.choices[0].message.tool_calls[0].function
    return json.loads(function_call.arguments)

# Example usage

result = get_stock_analysis_structured({
   
})
print(json.dumps(result, indent=2))