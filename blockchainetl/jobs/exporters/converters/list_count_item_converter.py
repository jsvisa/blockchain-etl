class ListCountItemConverter:
    def __init__(self, field, new_field_prefix):
        self.field = field
        self.new_field_prefix = new_field_prefix

    def convert_item(self, item):
        if not item:
            return item

        lst = item.get(self.field)
        if lst is not None and isinstance(lst, list):
            item[self.new_field_prefix + self.field] = len(lst)
        return item
