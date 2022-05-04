<template>
  <h5>Entity List</h5>
  <ul>
    <li v-for="(entity, number) in entities" :key="number">
      <EntityModal :gnd="entity.gnd" :nm="entity.nm" />
    </li>
  </ul>
</template>

<script>
import EntityModal from "@/components/EntityModal.vue";

export default {
  name: "EntityList.vue",
  components: {
    EntityModal,
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
