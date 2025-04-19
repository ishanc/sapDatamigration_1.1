from huggingface_hub import InferenceClient

client = InferenceClient(
    provider="hf-inference",
    token="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"
)

messages = [
    {
        "role": "user",
        "content": "What is the capital of France?"
    }
]

stream = client.chat.completions.create(
    model="mistralai/Mistral-Nemo-Instruct-2407", 
    messages=messages, 
    max_tokens=500,
    stream=True
)

for chunk in stream:
    print(chunk.choices[0].delta.content, end="")