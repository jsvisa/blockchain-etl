import collections


def group_by_item_type(items):
    result = collections.defaultdict(list)
    for item in items:
        result[item.get("type")].append(item)

    return result
