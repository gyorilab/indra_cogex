<template>
  <div v-if="user && user.input" class="row">
    <div class="col card">
      <div
        class="card-header msg-wrapper-header"
        :class="shown ? '' : 'border-bottom-0'"
        title="Click to toggle response visibility"
        @click="shown = !shown"
        data-bs-toggle="collapse"
        :data-bs-target="`#${componentID}`"
        aria-expanded="true"
        :aria-controls="componentID"
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
      <div class="collapse show" :id="componentID">
        <div class="card-body">
          <template v-if="bot && bot.text">
            <div class="row">
              <div class="col-1">
                <span class="text-muted small" :title="receivedDate"
                  >Output ({{ shortDate }})
                </span>
              </div>
              <div class="col-11 text-start">
                <span v-html="bot.text"></span>
              </div>
            </div>
          </template>
          <hr v-if="bot && bot.text && entityList.length" />
          <template v-if="entityList.length">
            <div class="row">
              <div class="col-1">
                <span class="text-muted small" :title="receivedDate"
                  >Entity list ({{ entityList.length }})
                </span>
              </div>
              <div
                class="col-11 text-start overflow-auto entity-list-container"
              >
                <EntityList :entities="entityList" />
              </div>
            </div>
          </template>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import EntityList from "@/components/EntityList.vue";
import getUUID from "@/helpers/helperFunctions";

export default {
  name: "MessageWrapper.vue",
  components: {
    EntityList,
  },
  data() {
    return {
      listMax: 0,
      shown: true,
    };
  },
  props: {
    bot: {
      /* Expecting a bot object:
       * {
       *  text: "",
       *  name: "",
       *  entities: [{gnd: "", nm: ""}, ...],
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
    entityList() {
      if (this.bot.entities) {
        return this.bot.entities;
      }
      return [];
    },
    componentID() {
      return `msgWrapper-${this.uuid}`;
    },
  },
  setup() {
    const uuid = getUUID()();
    return {
      uuid,
    };
  },
};
</script>

<style scoped>
.entity-list-container {
  max-height: 200px;
  overflow-y: auto;
}
.msg-wrapper-header {
  cursor: pointer;
}
</style>
