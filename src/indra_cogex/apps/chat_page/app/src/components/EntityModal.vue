<template>
  <!-- BootStrap 4.6 Modal -->
  <!-- Button type modal from anchor tag -->
  <a
    type="button"
    :title="title"
    @click="fillXrefs()"
    class="node-modal"
    data-toggle="modal"
    :data-target="`#${modalUUID}`"
  >
    <b>{{ nm }}</b>
  </a>

  <!-- Modal -->
  <div
    class="modal fade"
    :id="modalUUID"
    tabindex="-1"
    :aria-labelledby="titleUUID"
    aria-hidden="true"
  >
    <div class="modal-dialog">
      <div class="modal-content">
        <div class="modal-header">
          <h5 class="modal-title" :id="titleUUID">Modal title</h5>
          <button
            type="button"
            class="close"
            data-dismiss="modal"
            aria-label="Close"
          >
            <span aria-hidden="true">&times;</span>
          </button>
        </div>
        <div class="modal-body">
          <p v-if="entityDescription">
            <b>{{ nm }}</b
            ><br />{{ entityDescription }}
          </p>
          <p v-else><span>loading gif...</span></p>
          <hr />
          <table>
            <thead>
              <tr>
                <th>Database</th>
                <th>Identifier</th>
                <th>Lookup</th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="(xref, index) in allRefs"
                :key="`${modalUUID}-row${index}`"
              >
                <td>{{ xref.db }}</td>
                <td>{{ xref.id }}</td>
                <td>
                  <a :href="`${xref.url}`" target="_blank">
                    <i class="fas fa-external-link-alt">Link</i>
                  </a>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import getUUID from "@/helpers/helperFunctions.js";

export default {
  name: "EntityModal.vue",
  props: {
    nm: {
      type: String,
      required: true,
    },
    gnd: {
      // Array of db name, db id
      type: Array,
      required: true,
      validator: (arr) => {
        return arr.length === 2;
      },
    },
  },
  data() {
    return {
      xrefs: [],
      serverError: false,
      lookupData: {},
    };
  },
  computed: {
    title() {
      return `Grounded to ${this.gnd.join(
        ":"
      )}. Click to see more about the entity.`;
    },
    modalUUID() {
      return `modal-${this.uuid}`;
    },
    titleUUID() {
      return `modal-${this.uuid}-title`;
    },
    allRefs() {
      return [
        [
          this.gnd[0],
          this.gnd[1],
          `https://bioregistry.io/${this.gnd[0]}:${this.gnd[1]}`,
        ],
        ...this.xrefs,
      ];
    },
    entityDescription() {
      if (this.lookupData.definition) {
        return this.lookupData.definition;
      }
      return "";
    },
  },
  methods: {
    async fillXrefs() {
      // Todo: Use a global with `inject ['GStore']` to store the xrefs in
      // Call network search web api to get xrefs; fixme: should use a standalone api; Isn't there one for the bioontology?
      const xrefsUrl = `https://network.indra.bio/api/xrefs?ns=${this.gnd[0]}&id=${this.gnd[1]}`;
      const xrefResp = await fetch(xrefsUrl);
      console.log("Xrefs response");
      console.log(xrefResp);
      const xrefData = await xrefResp.json();
      this.xrefs = await xrefData;

      // Call biolookup.io, e.g. http://biolookup.io/api/lookup/DOID:14330
      const bioluUrl = `http://biolookup.io/api/lookup/${this.gnd[0]}:${this.gnd[1]}`; // Currently only supports http
      console.log("biolookup url: " + bioluUrl);
      const bioluResp = await fetch(bioluUrl);
      console.log("Lookup response");
      console.log(bioluResp);
      const bioluData = await bioluResp.json();
      this.lookupData = await bioluData;
    },
  },
  setup() {
    const uuid = getUUID();
    return {
      uuid,
    };
  },
};
</script>
