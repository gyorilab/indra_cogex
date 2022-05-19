<template>
  <div
    class="row"
    v-for="(stmt, index) in stmts"
    :key="`${this.componentUUID}-${index}`"
  >
    <div class="col">
      Agents:
      <span
        v-for="(ag, innerIndex) in this.getAgentList(stmt)"
        :key="`${this.componentUUID}-${index}-${innerIndex}`"
        ><AgentModal :agent-object="ag"
      /></span>
    </div>
    <div class="col text-end">
      See statement on
      <a
        :href="`https://discovery.indra.bio/statement_display?stmt_hash=${stmt.matches_hash}`"
        >discovery.indra.bio</a
      >
    </div>
  </div>
</template>

<script>
import AgentModal from "@/components/AgentModal.vue";
import helperFunctions from "@/helpers/helperFunctions";

export default {
  name: "StmtList.vue",
  components: {
    AgentModal,
  },
  props: {
    stmts: {
      /* Array of statements:
       * Each statement is an object with the following properties:
       *  - belief: Number
       *  - evidence: Array of evidence objects {
       *      - annotations: Object containing evidence annotations
       *      - epistemics: Object containing evidence epistemics
       *      - pmid: String of article PMID
       *      - source_api: String of source API name (e.g. "sparser", "reach", "biopax", "signor")
       *      - source_hash: Number of source hash (todo: this is larger than Number.MAX_SAFE_INTEGER, need to cast as string on server)
       *      - text: String of evidence text
       *      - text_refs: Object containing identifiers for different article sources, e.g. { "PMID": "12345", "doi": "10.1038/ncomms12456" }
       *    }
       *  - id: Hex String (what is this?)
       *  - matches_hash: String
       *  - obj: (optional) Object containing the statement object as an Agent: { name: "", db_refs: {...} }
       *  - obj_activity: (optional) String describing the activity of the object
       *  - subj: (optional) Object containing the statement subject as an Agent: { name: "", db_refs: {...} }
       *  - subj_activity: (optional) String describing the activity of the subject
       *  - type: String describing the statement type (e.g. "Activation", "Inhibition")
       */
      type: Array,
      required: true,
    },
  },
  computed: {
    componentUUID() {
      return `stmtList-${this.uuid}`;
    },
  },
  setup() {
    const uuid = helperFunctions.getUUID()();
    return {
      uuid,
    };
  },
  methods: {
    getAgentList(stmt) {
      const agentList = [];
      if (stmt.subj) {
        agentList.push(stmt.subj);
      }
      if (stmt.obj) {
        agentList.push(stmt.obj);
      }
      if (stmt.members && stmt.members.length > 0) {
        agentList.concat(stmt.members);
      }
      return agentList;
    },
  },
};
</script>
