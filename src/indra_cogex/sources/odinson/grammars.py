class Rule:
    def __init__(self, name, label, type, pattern):
        self.name = name
        self.label = label
        self.type = type
        self.pattern = pattern

    def __str__(self):
        return f"Rule({self.name}, {self.label}, {self.type}, {self.pattern})"

    def __repr__(self):
        return str(self)

    def compile(self):
        return (
            f"- name: {self.name}\n   label: {self.label}\n   "
            f"type: {self.type}\n   pattern: |\n    {self.pattern}"
        )


expressed_in_1 = Rule(
    name="expressed_in_1",
    label="Exp",
    type="basic",
    pattern="[lemma=express] >/nmod_in/ [entity=/B-.*/] [entity=/I-.*/]*",
)
