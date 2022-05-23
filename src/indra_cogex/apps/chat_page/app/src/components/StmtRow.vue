<!-- Todo:
      1. Clickable row to show:
        - AgentModals for agents in the row 
        - Statement linkout
        - Add preview in a modal?
        - Evidence info e.g. link to pubmed,
       -->
<template>
  <div
    class="card-header stmt-row-header"
    :class="shown ? '' : 'border-bottom-0'"
    @click="shown = !shown"
    title="Click to show/hide statement details"
    data-bs-toggle="collapse"
    :data-bs-target="'#' + collapseAreaID"
    aria-expanded="false"
    :aria-controls="collapseAreaID"
  >
    <span>{{ stmt.english }}</span>
  </div>
  <div class="collapse" :id="collapseAreaID">
    <div class="card-body">
      <div class="row">
        <div class="col-auto">
          <h5>Agents involved in this statements</h5>
          <span
            v-for="(ag, innerIndex) in agentList"
            :key="`${this.componentUUID}-${rowIndex}-${innerIndex}`"
            ><AgentModal :agent-object="ag"
          /></span>
        </div>
        <div class="col text-end">
          Full statement info on:
          <a
            :href="`https://discovery.indra.bio/statement_display?stmt_hash=${stmt.matches_hash}`"
            target="_blank"
            rel="noopener noreferrer"
            ><i class="bi bi-box-arrow-up-right"></i
          ></a>
        </div>
      </div>
      <div class="row">
        <h5>Evidence</h5>
        <div
          class="col"
          v-for="([sourceDb, dbId], evIndex) of Object.entries(
            stmt.evidence[0].text_refs
          )"
          :key="`${this.componentUUID}-${rowIndex}-${evIndex}`"
        >
          <b>{{ sourceDb }}</b
          >: {{ dbId }}
          <!-- Make link to source >: {{ dbId }}-->
        </div>
        <div class="row">
          <span class="text-center">Here put info about the paper</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import AgentModal from "@/components/AgentModal.vue";

export default {
  name: "StmtRow.vue",
  components: {
    AgentModal,
  },
  data() {
    return {
      shown: false,
    };
  },
  props: {
    stmt: {
      type: Object,
      required: true,
    },
    rowIndex: {
      type: Number,
      required: true,
    },
  },
  computed: {
    agentList() {
      // Get all agent objects from the statement
      return this.getAgentList(this.stmt);
    },
    componentUUID() {
      return this.stmt.id;
    },
    collapseAreaID() {
      return `collapse-area-${this.stmt.id}`;
    },
    collapseControlID() {
      return `collapse-control-${this.stmt.id}`;
    },
  },
  methods: {
    getAgentList(stmt) {
      // Initialize agent list
      const list = [];
      // Loop through key-value pairs of the statement object and add
      // to agentList the objects that have the keys "name" and "db_refs"
      for (const obj of Object.values(stmt)) {
        if (obj.name && obj.db_refs) {
          list.push(obj);
        }
      }
      return list;
    },
  },
};
</script>

<style scoped>
.stmt-row-header {
  cursor: pointer;
}
</style>
