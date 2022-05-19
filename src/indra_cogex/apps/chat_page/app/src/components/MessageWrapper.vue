<template>
  <div v-if="user && user.input" :id="componentID" class="row">
    <div class="col card mb-1">
      <div
        class="card-header msg-wrapper-header"
        :class="shown ? '' : 'border-bottom-0'"
        title="Click to expand/collapse response"
        @click="shown = !shown"
        data-bs-toggle="collapse"
        :data-bs-target="`#${idRegistry.collapseID}`"
        aria-expanded="true"
        :aria-controls="idRegistry.collapseID"
      >
        <div class="row">
          <div class="col-1">
            <span class="text-muted small" :title="user.createdAt"
              >User Input</span
            >
          </div>
          <div class="col-11 text-start">
            <span>{{ user.input }}</span>
          </div>
        </div>
      </div>
      <div class="collapse show" :id="idRegistry.collapseID">
        <div class="card-body">
          <div class="row mb-1">
            <div class="col-2 text-start">
              <span
                v-if="receivedDate"
                class="text-muted small"
                :title="receivedDate"
                >Output ({{ shortDate }})
              </span>
            </div>
            <div class="col-10"></div>
          </div>
          <div class="row">
            <nav>
              <div
                class="nav nav-tabs"
                :id="idRegistry.navTabsID"
                role="tablist"
              >
                <!-- Text tab -->
                <button
                  v-if="bot && bot.raw_text"
                  class="nav-link active"
                  :id="idRegistry.nav.textID"
                  data-bs-toggle="tab"
                  :data-bs-target="`#${idRegistry.content.textID}`"
                  type="button"
                  role="tab"
                  :aria-controls="idRegistry.content.textID"
                  aria-selected="true"
                >
                  Respone
                </button>
                <!-- Entities tab -->
                <button
                  v-if="replyEntities.length > 0"
                  class="nav-link"
                  @click.once="click.entities = true"
                  :title="receivedDate"
                  :id="idRegistry.nav.entitiesID"
                  data-bs-toggle="tab"
                  :data-bs-target="`#${idRegistry.content.entitiesID}`"
                  type="button"
                  role="tab"
                  :aria-controls="idRegistry.content.entitiesID"
                  aria-selected="false"
                >
                  Entities ({{ replyEntities.length }})
                </button>
                <!-- Stmts tab -->
                <button
                  v-if="replyStmts.length > 0"
                  class="nav-link"
                  @click.once="click.stmts = true"
                  title="See statements associated with this response"
                  :id="idRegistry.nav.stmtsID"
                  data-bs-toggle="tab"
                  :data-bs-target="`#${idRegistry.content.stmtsID}`"
                  type="button"
                  role="tab"
                  :aria-controls="idRegistry.content.stmtsID"
                  aria-selected="false"
                >
                  Statements ({{ replyStmts.length }})
                </button>
              </div>
            </nav>
            <!-- Tab content -->
            <div class="tab-content" :id="idRegistry.tabContentID">
              <!-- Text tab -->
              <div
                class="tab-pane fade show active"
                :id="idRegistry.content.textID"
                role="tabpanel"
                :aria-labelledby="idRegistry.nav.textID"
              >
                <div class="card card-body border-light">
                  <!-- Show spinner if receivedDate is null -->
                  <div
                    v-if="bot && !receivedDate"
                    class="d-flex align-items-center"
                  >
                    <span>Loading...</span>
                    <div
                      class="spinner-grow spinner-grow-sm text-secondary ms-auto"
                      role="status"
                      aria-hidden="true"
                    ></div>
                  </div>
                  <template v-if="bot && bot.raw_text">
                    <p class="text-start">
                      <i>Raw text: </i>
                      <span v-html="bot.raw_text"></span>
                    </p>
                    <p class="text-start">
                      <i>Formatted text: </i><br />
                      <template
                        v-for="(txtObj, index) in textObjectArray"
                        :key="`${idRegistry.content.textID}-text${index}`"
                      >
                        <!-- No object: just display the text as innerHtml -->
                        <span
                          v-if="txtObj.object === null"
                          v-html="txtObj.text"
                        ></span>
                        <!-- A single agent from query: display as AgentModal -->
                        <template
                          v-else-if="
                            txtObj.object.role === 'fixed' &&
                            txtObj.object.type === 'agent'
                          "
                        >
                          <AgentModal :agent-object="txtObj.object.value" />
                        </template>
                        <!-- A string list, set as an unordered list -->
                        <ul v-else-if="txtObj.object.type === 'string_list'">
                          <li
                            v-for="(str, index) in txtObj.object.value"
                            :key="`${idRegistry.content.textID}-strlist${index}`"
                          >
                            {{ str }}
                          </li>
                        </ul>
                        <!-- agent list with reply entities: display the computed string -->
                        <span
                          v-else-if="
                            txtObj.object.type === 'agent_list' &&
                            txtObj.object.role === 'variable'
                          "
                          v-html="txtObj.text"
                        ></span>
                        <!-- A single entity from query: display as EntityModal -->
                        <span v-else-if="txtObj.object.type === 'url_list'">
                          <span
                            v-for="([url, db_name], index) in txtObj.object
                              .value"
                            :key="`${idRegistry.content.textID}-urllist${index}`"
                          >
                            <a :href="url" target="_blank">{{ db_name }}</a>
                            {{
                              index === txtObj.object.value.length - 2
                                ? "and "
                                : ", "
                            }}
                          </span>
                        </span>
                      </template>
                    </p>
                  </template>
                </div>
              </div>
              <!-- Entities content -->
              <div
                v-if="replyEntities.length > 0"
                class="tab-pane fade entity-list-container"
                :id="idRegistry.content.entitiesID"
                role="tabpanel"
                :aria-labelledby="idRegistry.nav.entitiesID"
              >
                <template v-if="click.entities">
                  <AgentList :entities="replyEntities" />
                </template>
              </div>
              <!-- Stmts content -->
              <div
                v-if="replyStmts.length > 0"
                class="tab-pane fade stmt-list-container"
                :id="idRegistry.content.stmtsID"
                role="tabpanel"
                :aria-labelledby="idRegistry.nav.stmtsID"
              >
                <template v-if="click.stmts">
                  <!-- <StmtList :stmts="replyStmts" />-->
                  <i>Show {{ replyStmts.length }} statements</i>
                </template>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import AgentModal from "@/components/AgentModal.vue";
import AgentList from "@/components/AgentList.vue";
import helperFunctions from "@/helpers/helperFunctions";

export default {
  name: "MessageWrapper.vue",
  components: {
    AgentList,
    AgentModal,
  },
  data() {
    return {
      listMax: 0,
      shown: true,
      click: {
        text: false,
        entities: false,
      },
    };
  },
  props: {
    bot: {
      /* Expecting a bot object:
       * {
       *  raw_text: "",
       *  name: "",
       *  objects: {...},
       *  sender: "",
       *  receivedAt: "",
       * }
       * */
      type: Object,
      default: null,
    },
    user: {
      /* Expecting this object:
       * {
       *  input: "",
       *  name: "",
       *  sender_email: "",
       *  createdAt: "",
       * }
       */
      type: Object,
      default: null,
    },
  },
  computed: {
    receivedDate() {
      if (this.bot.receivedAt !== null) {
        return new Date(this.bot.receivedAt).toLocaleString();
      }
      return "";
    },
    shortDate() {
      // Format: "HH:mm" 24 hour clock
      if (this.bot.receivedAt !== null) {
        return new Date(this.bot.receivedAt).toLocaleTimeString("en-GB", {
          hour: "2-digit",
          minute: "2-digit",
        });
      }
      return "";
    },
    objects() {
      if (this.bot && this.bot.objects) {
        return this.bot.objects;
      }
      return null;
    },
    queryEntities() {
      // Loop through the objects and check if they have role === 'fixed'
      if (this.bot && this.bot.objects) {
        return this.getObjectsWithTypeRole(
          this.bot.objects,
          "agent_list",
          "fixed"
        );
      }
      return [];
    },
    replyEntities() {
      // Loop through the objects and check if they have role === 'variable'
      if (this.bot && this.bot.objects) {
        return this.getObjectsWithTypeRole(
          this.bot.objects,
          "agent_list",
          "variable"
        );
      }
      return [];
    },
    replyStmts() {
      // Loop through the objects and check if they have role === 'statement'
      if (this.bot && this.bot.objects) {
        return this.getObjectsWithTypeRole(
          this.bot.objects,
          "statement_list",
          "variable"
        );
      }
      return [];
    },
    textObjectArray() {
      // Locate all references to text objects in raw text

      // Contains objects with entries: "text", "object"
      // "text" is the text to show, "object" is the object representing the text (null if plain text)
      let txtObjs = [];
      const rawText = this.bot.raw_text || "";

      // Split string on any text between '{' and '}'
      let txtObjStrings = rawText.split(/(\{.*?\})/g);

      if (txtObjStrings.length > 0 && this.bot.objects !== null) {
        // Loop through each string and match against objects
        txtObjStrings.forEach((substring) => {
          // Skip empty strings
          if (substring.length > 0) {
            // If we match a recognised object, look it up among the objects
            if (substring.includes("{") && substring.includes("}")) {
              let objName = substring.replace(/[{}]/g, "");
              let limit;
              // Check if there is a limit
              if (objName.includes(":")) {
                [objName, limit] = objName.split(":");
              } else {
                limit = null;
              }
              const obj = this.bot.objects[objName] || null;
              let textForObj;
              if (obj) {
                let formatSpec;
                if (limit !== null) {
                  // Get the numeric value of the limit, e.g. "limit10" -> 10
                  limit = parseInt(limit.replace(/limit/g, ""), 10);
                  formatSpec = { limit: limit };
                }
                textForObj = this.getTextForObj(obj, formatSpec);
                txtObjs.push({
                  text: textForObj,
                  object: obj,
                  objName: objName,
                  match: substring, // The string that matched the object
                });
              }
            } else {
              // If we don't match a recognised object, just add the string
              txtObjs.push({
                text: substring,
                object: null,
                objName: null,
                match: substring,
              });
            }
          }
        });
      } else {
        console.log("Shouldn't get here");
        if (rawText.includes("{") || rawText.includes("}")) {
          console.warn(
            "MessageWrapper.vue: No text objects found in raw text, but references found. Returning raw text."
          );
        } else {
          console.log("No text objects found");
        }
        txtObjs.push({
          text: this.bot.raw_text,
          object: null,
          objName: null,
          match: null,
        });
      }
      return txtObjs;
    },
    componentID() {
      return `msgWrapper-${this.uuid}`;
    },
    idRegistry() {
      return {
        collapseID: `${this.componentID}-collapse`,
        navTabsID: `${this.componentID}-nav-tabs`,
        tabContentID: `${this.componentID}-tabs-content`,
        nav: {
          textID: `${this.componentID}-tab-text`,
          entitiesID: `${this.componentID}-tab-entities`,
          stmtsID: `${this.componentID}-tab-stmts`,
        },
        content: {
          textID: `${this.componentID}-content-text`,
          entitiesID: `${this.componentID}-content-entities`,
          stmtsID: `${this.componentID}-content-stmts`,
        },
      };
    },
  },
  methods: {
    isActive(tab) {
      // Check if the tab is active
      return this.activeTab === tab;
    },
    englishJoin(arr, delimiter = ", ", oxford = true) {
      // Join an array of strings into a string with a delimiter and a grammatically correct conjunction
      if (arr.length === 0) {
        console.warn("No array to join", arr);
      } else if (arr.length === 1) {
        return arr[0];
      } else if (arr.length === 2) {
        return `${arr[0]} and ${arr[1]}`;
      } else {
        return `${arr.slice(0, -1).join(delimiter)}${
          oxford ? "," : ""
        } and ${arr.slice(-1)}`;
      }
    },
    getObjectsWithTypeRole(objs, type, role) {
      // Get all entities from type 'agent_list' with a given role
      let entities = [];
      // Loop the entries in the Object
      Object.values(objs).forEach((obj) => {
        if (obj.type === type && obj.role === role) {
          // Concatenate the entities (stored in the 'value' field)
          entities = entities.concat(obj.value);
        }
      });
      return entities;
    },
    getTextForObj(obj, format_spec = null) {
      // Return the text for the object
      switch (obj.type) {
        // Todo: handle 'stmt_list', 'url', 'url_list', 'string_list', 'str'
        case "agent":
          // Return object.value.name
          return obj.value.name;
        case "agent_list": {
          // If there is a limit, return the first N agent names as a string
          let limit;
          if (format_spec) {
            limit = format_spec.limit;
          }
          limit = limit || obj.value.length;
          let nameArray = obj.value.slice(0, limit).map((agent) => agent.name);
          return this.englishJoin(nameArray);
        }
        case "string_list":
        case "url_list":
          /* string_list, url_list is an arrays of strings that are joined together with
           * a delimiter in the template and doesn't need any text */
          return null;
        case "string":
          // Return the string
          return obj.value;
        default:
          console.log("In switch-case; unhandled object type: " + obj.type);
          console.log(obj);
          return "(N/A)";
      }
    },
  },
  setup() {
    const uuid = helperFunctions.getUUID()();
    return {
      uuid,
    };
  },
};
</script>

<style scoped>
.entity-list-container {
  max-height: 300px;
  overflow-y: auto;
}
.msg-wrapper-header {
  cursor: pointer;
}
</style>
