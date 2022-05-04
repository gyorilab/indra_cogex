<template>
  <div class="col card">
    <div class="card-body">
      <div class="row">
        <div class="col-1">
          <span class="text-muted small" :title="user_message.createdAt"
            >User Input</span
          >
        </div>
        <div class="col-11 text-start">
          <span>{{ user_message.input }}</span>
        </div>
      </div>
      <hr />
      <div class="row">
        <div class="col-1">
          <span class="text-muted small" :title="receivedDate"
            >Output ({{ shortDate }})
          </span>
        </div>
        <div class="col-11 text-start">
          <span v-html="message.text"></span>
        </div>
      </div>
      <template v-if="entityList.length">
        <hr />
        <div class="row">
          <div class="col-1">
            <span class="text-muted small" :title="receivedDate"
              >Entity list ({{ entityList.length }})
            </span>
          </div>
          <div class="col-11 text-start">
            <EntityList :entities="entityList" />
          </div>
        </div>
      </template>
    </div>
  </div>
</template>

<script>
import EntityList from "@/components/EntityList.vue";

export default {
  name: "MessageWrapper.vue",
  components: {
    EntityList,
  },
  data() {
    return {
      listMax: 0,
    };
  },
  props: {
    message: {
      /* Expecting a message object:
       * {
       *  text: "",
       *  name: "",
       *  entities: [{gnd: "", nm: ""}, ...],
       *  sender: "",
       *  receivedAt: "",
       * }
       * */
      type: Object,
      required: true,
    },
    user_message: {
      /* Expecting this object:
       * {
       *  input: "",
       *  name: "",
       *  sender_email: "",
       *  createdAt: "",
       * }
       */
      type: Object,
      required: true,
    },
  },
  computed: {
    receivedDate() {
      return new Date(this.message.receivedAt).toLocaleString();
    },
    shortDate() {
      // Format: "HH:mm" 24 hour clock
      return new Date(this.message.receivedAt).toLocaleTimeString("en-GB", {
        hour: "2-digit",
        minute: "2-digit",
      });
    },
    entityList() {
      if (this.message.entities) {
        return this.message.entities;
      }
      return [];
    },
  },
};
</script>

<style scoped></style>
