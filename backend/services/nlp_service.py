def extract_skills(text):
    keywords = ["python", "sql", "machine learning", "docker", "nlp"]

    found = []

    text = text.lower()

    for k in keywords:
        if k in text:
            found.append(k)

    return found