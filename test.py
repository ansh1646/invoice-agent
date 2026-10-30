import os
from decisions import call_llm

response = call_llm(
    "You are a helpful assistant. Return JSON only.",
    'Return {"status": "connected", "provider": "Gemini"}'
)

print(response)