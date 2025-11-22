import os
from openai import OpenAI
from dotenv import load_dotenv

# initialize client
def load_client():
    load_dotenv()
    return OpenAI(
        api_key=API_KEY,
        base_url=API_URL
    )

# dummy input creation
def get_document_context():
    with open("dummy_document.txt") as f:
        return f.read()

# prompt file
def load_prompt(context):
    with open("system_prompt.txt") as f:
        return f.read().replace("{context}", context)

# ask queries
def ask(client, system_prompt, query):
    try:
        response = client.chat.completions.create(
            model="Qwen3-32B",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": query}
            ],
            max_tokens=512,
            temperature=0.1
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"Error: {e}"

# main execution
if __name__ == "__main__":
    client = load_client()
    context = get_document_context()
    prompt = load_prompt(context)
    query = "What was U003's largest transaction?"
    print("Query:", query)
    print("Response:", ask(client, prompt, query))