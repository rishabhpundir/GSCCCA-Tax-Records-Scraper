import re
from typing import List, Dict

def normalize_for_compare(text: str) -> List[str]:
    """Uppercase + remove commas for comparison only."""
    text = text.upper()
    text = re.sub(r"[,\s]+", " ", text)
    return text.strip().split()


def merge_address_pair(addr1: str, addr2: str) -> str:
    """
    Merge two similar addresses without losing data
    and without repeating tokens.
    """
    # Decide which one is more detailed (longer token count)
    tokens1 = normalize_for_compare(addr1)
    tokens2 = normalize_for_compare(addr2)

    if len(tokens2) > len(tokens1):
        base = addr2
        other = addr1
        base_tokens = tokens2
        other_tokens = tokens1
    else:
        base = addr1
        other = addr2
        base_tokens = tokens1
        other_tokens = tokens2

    # Preserve original base formatting
    merged_tokens = base.split()

    # Build comparison set from normalized base
    base_token_set = set(base_tokens)

    # Check missing tokens from other
    for token in other.split():
        norm_token = re.sub(r"[,\s]+", "", token).upper()
        if norm_token not in base_token_set:
            merged_tokens.append(token)

    return " ".join(merged_tokens)


def merge_address_lists(addr_list_1: List[Dict], addr_list_2: List[str]) -> str:
    merged_results = []

    for addr1_obj in addr_list_1:
        addr1 = addr1_obj["address"]

        best_match = None
        for addr2 in addr_list_2:
            # crude similarity check (city+zip match)
            if addr1_obj["zipcode"] in addr2:
                best_match = addr2
                break

        if best_match:
            merged = merge_address_pair(addr1, best_match)
        else:
            merged = addr1

        merged_results.append(merged)

    return " | ".join(merged_results)

