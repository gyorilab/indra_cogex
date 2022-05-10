<template>
  <h5 v-if="showTitle">Entity List</h5>
  <div>
    <span
      v-for="([cls, descr], index) in availableClasses"
      class="badge"
      :title="availableClasses.length > 1 ? 'Toggle visibility' : ''"
      @click="toggleHide(cls)"
      :class="`${cls} ${isClsVisible(cls) ? '' : ' opacity-50'}`"
      :key="index"
      >{{ descr }}</span
    >
  </div>
  <hr />
  <div class="text-start">
    <template v-for="(entity, number) in computedList" :key="number">
      <EntityModal :gnd="entity.gnd" :nm="entity.nm" />
    </template>
  </div>
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
      primaryVis: true,
      secondaryVis: true,
      successVis: true,
      infoVis: true,
      lightVis: true,
      warningVis: true,
    };
  },
  props: {
    entities: {
      // Array of entity objects
      type: Array,
      required: true,
    },
    showTitle: {
      // Show title
      type: Boolean,
      default: true,
    },
  },
  computed: {
    computedList() {
      let list = [];
      for (let entity of this.entities) {
        if (this.isClsVisible(this.nsToCls(entity.gnd))) {
          list.push(entity);
        }
      }
      return list;
    },
    availableClasses() {
      let classes = [];
      for (const entity of this.entities) {
        const cls = this.nsToCls(entity.gnd);
        // Add class if it: is in badgeClasses and is not already in classes
        if (
          this.badgeClasses.some((el) => el[0] === cls) &&
          !classes.some((el) => el[0] === cls)
        ) {
          const clsDesc = this.badgeClasses.find((el) => el[0] === cls);
          classes.push(clsDesc);
        }
      }
      return classes;
    },
  },
  methods: {
    toggleHide(cls) {
      // If there is only one class, don't toggle
      if (this.availableClasses.length <= 1) {
        return;
      }
      switch (cls) {
        case "bg-primary":
          this.primaryVis = !this.primaryVis;
          break;
        case "bg-secondary":
          this.secondaryVis = !this.secondaryVis;
          break;
        case "bg-success":
          this.successVis = !this.successVis;
          break;
        case "bg-info text-dark":
          this.infoVis = !this.infoVis;
          break;
        case "bg-light text-dark":
          this.lightVis = !this.lightVis;
          break;
        case "bg-warning text-dark":
          this.warningVis = !this.warningVis;
          break;
      }
    },
    isClsVisible(cls) {
      switch (cls) {
        case "bg-primary":
          return this.primaryVis;
        case "bg-secondary":
          return this.secondaryVis;
        case "bg-success":
          return this.successVis;
        case "bg-info text-dark":
          return this.infoVis;
        case "bg-light text-dark":
          return this.lightVis;
        case "bg-warning text-dark":
          return this.warningVis;
      }
    },
    nsToCls(gnd) {
      let ns = "";
      if (gnd[0]) {
        ns = gnd[0].toLowerCase();
      }
      switch (ns) {
        case "fplx":
        case "hgnc":
        case "up":
        case "uppro":
        case "mirbase":
          return "bg-primary";
        case "chebi":
          return "bg-secondary";
        case "go":
        case "mesh":
        case "doid":
          return "bg-success";
        case "hp":
          return "bg-info text-dark";
        case "efo":
          return "bg-light text-dark";
        default:
          console.warn("Unknown namespace: " + ns, gnd);
          return "warning text-dark";
      }
    },
  },
};
</script>

<style scoped>
.badge {
  cursor: pointer;
}
</style>
