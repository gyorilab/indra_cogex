<template>
  <div v-if="user && user.input" :id="componentID" class="row">
    <div class="col card mb-3">
      <div
        class="card-header msg-wrapper-header"
        :class="shown ? '' : 'border-bottom-0'"
        title="Click to expand/collapse response"
        @click="shown = !shown"
        data-bs-toggle="collapse"
        :data-bs-target="`#${idRegistry.collapse}`"
        aria-expanded="true"
        :aria-controls="idRegistry.collapse"
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
      <div class="collapse show" :id="idRegistry.collapse">
        <div class="card-body">
          <div class="row mb-1">
            <div class="col-2 text-start">
              <span class="text-muted small" :title="receivedDate"
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
                  v-if="bot && bot.text"
                  class="nav-link active"
                  :id="idRegistry.navTabs.nav.text"
                  data-bs-toggle="tab"
                  :data-bs-target="`#${idRegistry.navTabs.content.text}`"
                  type="button"
                  role="tab"
                  :aria-controls="idRegistry.navTabs.content.text"
                  aria-selected="true"
                >
                  Respone
                </button>
                <!-- Entities tab -->
                <button
                  v-if="replyEntities.length"
                  class="nav-link"
                  @click.once="click.entities = true"
                  :title="receivedDate"
                  :id="idRegistry.navTabs.nav.entities"
                  data-bs-toggle="tab"
                  :data-bs-target="`#${idRegistry.navTabs.content.entities}`"
                  type="button"
                  role="tab"
                  :aria-controls="idRegistry.navTabs.content.entities"
                  aria-selected="false"
                >
                  Entities ({{ replyEntities.length }})
                </button>
              </div>
            </nav>
            <div class="tab-content" :id="idRegistry.tabsContent">
              <!-- Text content -->
              <div
                v-if="bot && bot.text"
                class="tab-pane fade show active"
                :id="idRegistry.navTabs.content.text"
                role="tabpanel"
                :aria-labelledby="idRegistry.navTabs.nav.text"
              >
                <div class="card card-body border-light">
                  <p class="text-start" v-html="bot.text"></p>
                </div>
              </div>
              <!-- Entities content -->
              <div
                v-if="replyEntities.length"
                class="tab-pane fade show active entity-list-container"
                :id="idRegistry.navTabs.content.entities"
                role="tabpanel"
                :aria-labelledby="idRegistry.navTabs.nav.entities"
              >
                <template v-if="click.entities">
                  <EntityList :entities="replyEntities" />
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
import EntityList from "@/components/EntityList.vue";
import helperFunctions from "@/helpers/helperFunctions";

export default {
  name: "MessageWrapper.vue",
  components: {
    EntityList,
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
       *  text: "",
       *  name: "",
       *  query_entities: [{gnd: "", nm: ""}, ...],
       *  reply_entities: [{gnd: "", nm: ""}, ...],
       *  stmt_list: [{al: [nm, ...], tp: "", hs: ""}, ...],
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
      return new Date(this.bot.receivedAt).toLocaleString();
    },
    shortDate() {
      // Format: "HH:mm" 24 hour clock
      return new Date(this.bot.receivedAt).toLocaleTimeString("en-GB", {
        hour: "2-digit",
        minute: "2-digit",
      });
    },
    queryEntities() {
      if (this.bot && this.bot.query_entities) {
        return this.bot.query_entities;
      }
      return [];
    },
    replyEntities() {
      if (this.bot && this.bot.reply_entities) {
        return this.bot.reply_entities;
      }
      return [];
    },
    stmtList() {
      if (this.bot && this.bot.stmt_list) {
        return this.bot.stmt_list;
      }
      return [];
    },
    componentID() {
      return `msgWrapper-${this.uuid}`;
    },
    idRegistry() {
      return {
        collapse: `${this.componentID}-collapse`,
        navTabsID: `${this.componentID}-nav-tabs`,
        tabsContent: `${this.componentID}-tabs-content`,
        navTabs: {
          nav: {
            text: `${this.componentID}-tab-text`,
            entities: `${this.componentID}-tab-entities`,
          },
          content: {
            text: `${this.componentID}-content-text`,
            entities: `${this.componentID}-content-entities`,
          },
        },
      };
    },
  },
  methods: {
    setTabActive(tab) {
      // Set the active tab
      this.activeTab = tab;
    },
    isActive(tab) {
      // Check if the tab is active
      return this.activeTab === tab;
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
