const badgeMappings = {
  ["bg-primary"]: ["fplx", "hgnc", "up", "uppro", "mirbase"], // genes, proteins
  ["bg-secondary"]: ["chebi", "pubchem", "chembl", "hms-lincs"], // Small molecule
  ["bg-success"]: ["go", "mesh", "doid"], // Biological process, disease
  ["bg-info text-dark"]: ["hp"], // Phenotypic Abnormality
  ["bg-light text-dark"]: ["efo"], // Experimental Factor Ontology
};
// "text" is the default badge class and should be the last one
const nsPriority = [
  "fplx",
  "uppro",
  "hgnc",
  "up",
  "chebi",
  "go",
  "mesh",
  "mirbase",
  "doid",
  "hp",
  "efo",
];
const nsPriorityMap = {};
nsPriority.forEach((ns, i) => {
  nsPriorityMap[ns] = i;
});
nsPriorityMap["default"] = nsPriority.length;

export const DefaultValues = {
  badgeMappings,
  nsPriority,
  nsPriorityMap,
};
