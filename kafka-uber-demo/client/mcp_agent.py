import openai

openai.api_key = "your-api-key"

prompt = """
You are an AI dispatcher. You have 3 drivers:
- Alice (1 km, 4.5⭐)
- Bob (2 km, 5⭐)
- Charlie (0.5 km, busy)

Who should you assign a ride from 'MG Road'?
"""

res = openai.ChatCompletion.create(
    model="gpt-4",
    messages=[{"role": "system", "content": prompt}]
)

print("🧠 AI Dispatcher Decision:\n", res['choices'][0]['message']['content'])

