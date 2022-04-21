<template>
  <!-- Add horizontal line -->
  <hr />
  <h3>Discovery App</h3>
  <template v-if="logged_in">
    <p>Logged in as {{ chat.name }} ({{ chat.email }})</p>
    <input type="text" v-model="text_input" :disabled="disable_input" />
    <button @click="sendMessage" :disabled="disable_input">Send</button>
    <ul id="chatList" class="clearfix messages">
      <li
        class="clearfix message"
        v-for="(message, index) in chat.messages"
        :key="index"
      >
        <span>{{ message.name }}:</span>
        <span>{{ message.text }}</span>
      </li>
    </ul>
  </template>
  <template v-else>
    <form id="loginForm" @submit.prevent="logIntoChatSession">
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
  </template>
  <div>
    <p>The pusher key is "{{ pusher_key }}"</p>
  </div>
</template>

<script>
import Pusher from "pusher-js";

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
        messages: [],
      },
      submitted: false,
      logged_in: false,
      disable_input: false,
      text_input: "",
    };
  },
  mounted() {
    this.getAppInfo();
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
      const data = await resp.json();
      this.pusher_info = await data;
      // Setup pusher instance
      this.pusher = await this.setupPusher();
      // Subscribe to the channel
      this.pusher.bind("client-support-new-message", (data) => {
        this.newMessage(data);
      });
    },
    async setupPusher() {
      return new Pusher(this.pusher_key, {
        authEndpoint: this.auth_endpoint,
        cluster: this.pusher_cluster,
        encrypted: true,
      });
    },
    async logIntoChatSession() {
      if (this.chat.name && this.chat.email) {
        // Disable the form
        this.submitted = true;
        // Trim strings
        let trimmed_name = this.chat.name.trim();
        let trimmed_email = this.chat.email.trim();
        const resp = await fetch(this.new_user_endpoint, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            name: trimmed_name,
            email: trimmed_email,
          }),
        });
        const data = await resp.json();
        const user_info = await data;
        this.chat.channel = user_info.channel;
        this.logged_in = true;
      }
    },
    sendMessage() {
      if (this.chat.channel) {
        this.disable_input = true;
        const createdAt = new Date().toUTCString();
        const message = {
          sender: this.chat.name,
          email: this.chat.email,
          text: this.text_input,
          createdAt: createdAt,
        };
        this.pusher.trigger("client-guest-new-message", message);
        this.newMessage({
          text: message.text,
          name: message.sender,
          sender: message.email,
        });
        this.text_input = "";
      }
    },
    newMessage(message) {
      console.log("New message");
      console.log(message);
      if (message !== undefined) {
        // Add the message to the chat
        this.chat.messages.push(message);
      }
    },
  },
};
</script>

<style scoped></style>
