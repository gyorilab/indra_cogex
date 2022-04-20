<template>
  <!-- Add horizontal line -->
  <hr />
  <h3>Discovery</h3>
  <div v-if="logged_in">
    <p>Logged in as {{ chat.name }} ({{ chat.email }})</p>
    <input type="text" v-model="chat.message" @keyup.enter="sendMessage" />
  </div>
  <div v-else>
    <form id="loginForm">
      <div class="form-row">
        <div class="form-group col-md-6">
          <label for="name">Name</label>
          <input
            type="text"
            class="form-control"
            id="name"
            placeholder="Name*"
            v-model="chat.name"
            :disabled="submitted"
            required
          />
        </div>
        <div class="form-group col-md-6">
          <label for="email">Email</label>
          <input
            type="email"
            class="form-control"
            id="email"
            placeholder="Email*"
            v-model="chat.email"
            :disabled="submitted"
            required
          />
        </div>
      </div>
      <div class="form-group form-row">
        <button
          type="submit"
          class="btn btn-block btn-primary"
          :disabled="disableBtn"
        >
          Start Session
        </button>
      </div>
    </form>
  </div>
  <pre>
    <code>
      {{ pusher_info }}
    </code>
  </pre>
</template>

<script>
import Pusher from "pusher-js/with-encryption";

export default {
  name: "DiscoveryApp.vue",
  data() {
    return {
      /** Pusher options and related data
       * {
       *    "pusher_key": pusher_key or "",
       *    "pusher_cluster": pusher_cluster or "",
       *    "auth_endpoint": url_for(".pusher_authentication"),
       *    "new_user_endpoint": url_for(".guestUser"),
       * }
       */
      pusher_info: {},
      pusher: null,
      chat: {
        name: undefined,
        email: undefined,
        channel: undefined,
      },
      submitted: false,
      logged_in: false,
    };
  },
  async mounted() {
    await this.getAppInfo();
  },
  computed: {
    pusher_key() {
      if (this.pusher_info) {
        return this.pusher_info.pusher_key;
      }
      return "";
    },
    pusher_cluster() {
      if (this.pusher_info) {
        return this.pusher_info.pusher_cluster;
      }
      return "";
    },
    auth_endpoint() {
      if (this.pusher_info) {
        return this.pusher_info.auth_endpoint;
      }
      return "";
    },
    new_user_endpoint() {
      if (this.pusher_info) {
        return this.pusher_info.new_user_endpoint;
      }
      return "";
    },
    disableBtn() {
      const unfilled = !this.chat.name || !this.chat.email;
      return unfilled || this.submitted;
    },
  },
  methods: {
    async getAppInfo() {
      const resp = await fetch("/chat/pusher_info", {
        method: "GET",
      });
      console.log(resp);
      this.pusher_info = await resp.json();
    },
    async setupPusher() {
      if (!this.pusher_key) {
        await this.getAppInfo();
      }
      return new Pusher(this.pusher_key(), {
        authEndpoint: this.auth_endpoint,
        cluster: this.pusher_cluster,
        encrypted: true,
      });
    },
    async logIntoChatSession() {
      if (this.chat.name && this.chat.email) {
        // Disable the form
        this.submitted = true;
        //
        const resp = await fetch(this.new_user_endpoint, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            name: this.chat.name,
            email: this.chat.email,
          }),
        });
        const user_info = await resp.json();
        this.chat.channel = user_info.channel;
      }
    },
  },
};
</script>

<style scoped></style>
