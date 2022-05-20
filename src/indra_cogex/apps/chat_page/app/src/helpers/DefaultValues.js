const badgeMappings = {
  ["bg-primary"]: ["fplx", "hgnc", "up", "uppro", "mirbase", "egid"], // genes, proteins
  ["bg-secondary"]: [
    "chebi",
    "pubchem",
    "chembl",
    "hms-lincs",
    "lincs",
    "drugbank",
  ], // Small molecule
  ["bg-success"]: ["go", "mesh", "doid"], // Biological process, disease
  ["bg-info text-dark"]: ["hp"], // Phenotypic Abnormality
  ["bg-light text-dark"]: ["efo"], // Experimental Factor Ontology
};
// "text" is the default badge class and should be the last one
// Check e.g. indra/ontology/bio/ontology.py for more info about prioritizing namespaces
const nsPriority = [
  "fplx",
  "uppro",
  "hgnc",
  "up",
  "chebi",
  "go",
  "mesh",
  "mirbase",
  "egid",
  "doid",
  "hp",
  "efo",
  "pubchem",
  "hms-lincs",
  "chembl",
  "lincs",
  "drugbank",
  "text",
];
const nsPriorityMap = {};
nsPriority.forEach((ns, i) => {
  // Set priority to the 1-indexed position of the namespace in the array
  nsPriorityMap[ns] = i + 1;
});
nsPriorityMap["default"] = nsPriority.length;

export const DefaultValues = {
  badgeMappings,
  nsPriority,
  nsPriorityMap,
};
