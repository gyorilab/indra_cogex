__all__ = ["Node", "Relation"]


class Node:
    def __init__(self, identifier, labels, data=None):
        self.identifier = identifier
        self.labels = labels
        self.data = data if data else {}

    def to_json(self):
        data = {k: v for k, v in self.data.items()}
        data["id"] = self.identifier
        return {"labels": self.labels, "data": data}

    def get_data_str(self):
        pieces = ["id:'%s'" % self.identifier]
        for k, v in self.data.items():
            if isinstance(v, str):
                value = "'" + v.replace("'", "\\'") + "'"
            elif isinstance(v, (bool, int, float)):
                value = v
            else:
                value = str(v)
            piece = "%s:%s" % (k, value)
            pieces.append(piece)
        data_str = ", ".join(pieces)
        return data_str

    def __str__(self):
        data_str = self.get_data_str()
        labels_str = ":".join(self.labels)
        return f"(:{labels_str} {{ {data_str} }})"

    def __repr__(self):
        return str(self)


class Relation:
    def __init__(self, source_id, target_id, labels, data=None):
        self.source_id = source_id
        self.target_id = target_id
        self.labels = labels
        self.data = data if data else {}

    def to_json(self):
        return {
            "source_id": self.source_id,
            "target_id": self.target_id,
            "labels": self.labels,
            "data": self.data,
        }

    def __str__(self):
        data_str = ", ".join(["%s:'%s'" % (k, v) for k, v in self.data.items()])
        labels_str = ":".join(self.labels)
        return f"({self.source_id})-[:{labels_str} {data_str}]->" f"({self.target_id})"

    def __repr__(self):
        return str(self)
