<template>
  <h5>Entity List</h5>
  <span
    v-for="([cls, descr], index) in badgeClasses"
    class="badge"
    :class="cls"
    :key="index"
    >{{ descr }}</span
  >
  <hr />
  <template v-for="(entity, number) in entities" :key="number">
    <EntityModal :gnd="entity.gnd" :nm="entity.nm" />
  </template>
</template>

<script>
import EntityModal from "@/components/EntityModal.vue";

export default {
  name: "EntityList.vue",
  components: {
    EntityModal,
  },
  data() {
    return {
      badgeClasses: [
        ["bg-primary", "Gene/Protein"],
        ["bg-secondary", "Small Molecule"],
        ["bg-success", "Biological Process, Disease"],
        ["bg-info text-dark", "Phenotypic Abnormality"],
        ["bg-light text-dark", "Experimental Factor"],
        ["bg-warning text-dark", "ungrounded"],
      ],
    };
  },
  props: {
    entities: {
      // Array of entity objects
      type: Array,
      required: true,
    },
  },
};
</script>
