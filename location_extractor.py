import re
import spacy

nlp = spacy.load("xx_ent_wiki_sm")

LOCATION_PATTERNS = [
    r"desa\s+[A-Z][a-z]+",
    r"kelurahan\s+[A-Z][a-z]+",
    r"kecamatan\s+[A-Z][a-z]+",
    r"kabupaten\s+[A-Z][a-z]+",
    r"kota\s+[A-Z][a-z]+",
    r"jalan\s+[A-Z][^\s,]+",
    r"gunung\s+[A-Z][a-z]+",
    r"bukit\s+[A-Z][a-z]+",
    r"tukad\s+[A-Z][a-z]+",
]

COMPILED = [re.compile(p, re.IGNORECASE) for p in LOCATION_PATTERNS]


def extract_locations(text: str):
    locations = []

    # -------- Rule-based detection ----------
    for pat in COMPILED:
        matches = pat.findall(text)
        locations.extend(matches)

    # -------- NER detection ----------
    doc = nlp(text)
    for ent in doc.ents:
        if ent.label_ in ("LOC", "GPE"):
            locations.append(ent.text)

    # -------- Normalize ----------
    cleaned = [loc.strip().title() for loc in locations]

    result = sorted(
        list(set(cleaned)),
        key=lambda x: cleaned.count(x),
        reverse=True
    )

    return result