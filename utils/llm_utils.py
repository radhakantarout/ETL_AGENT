import os
import anthropic

def get_claude_client():
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("Please set ANTHROPIC_API_KEY in environment.")
    return anthropic.Anthropic(api_key=api_key)

def generate_code_with_claude(prompt, max_tokens=2000, model="claude-sonnet-4-20250514"):
    """
    Generate Python code (or any text) using Claude via Anthropic SDK.
    """
    client = get_claude_client()

    # SDK call returns a Message object, not a dict
    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}]
    )

    # Access the generated text correctly
    if hasattr(response, "completion") and response.completion:
        return response.completion
    elif hasattr(response, "content") and response.content:
        return response.content
    else:
        return ""
