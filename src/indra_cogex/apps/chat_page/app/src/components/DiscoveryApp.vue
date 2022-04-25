<template>
  <!-- Add horizontal line -->
  <hr />
  <h3>Discovery App</h3>
  <template v-if="logged_in">
    <p>Logged in as {{ chat.name }} ({{ chat.email }})</p>
    <div class="input-group mb-3">
      <input
        type="text"
        class="form-control"
        placeholder="Enter question"
        aria-label="Enter question"
        aria-describedby="button-addon"
        v-model="text_input"
        :disabled="disable_input"
        @keyup.enter="sendMessage"
      />
      <div class="input-group-append">
        <button
          class="btn btn-outline-secondary"
          type="button"
          id="button-addon"
          @click="sendMessage"
          :disabled="disable_input"
        >
          Ask
        </button>
      </div>
    </div>
    <div id="chatList" class="clearfix messages">
      <div
        class="clearfix message row"
        v-for="(message, index) in chat_messages"
        :key="index"
      >
        <MessageWrapper :message="message" />
      </div>
    </div>
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
            v-model="form_name"
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
            v-model="form_email"
            :disabled="submitted"
            required
          />
        </div>
      </div>
      <div class="form-group form-row">
        <button type="submit" class="btn btn-block btn-primary">
          Start Session
        </button>
      </div>
    </form>
  </template>
</template>

<script>
import Pusher from "pusher-js";
import MessageWrapper from "@/components/MessageWrapper.vue";

export default {
  name: "DiscoveryApp.vue",
  components: { MessageWrapper },
  props: {
    info_endpoint: {
      type: String,
      default: "/chat/pusher_info",
    },
  },
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
      form_name: "",
      form_email: "",
      chat: {
        name: undefined,
        email: undefined,
        channel: undefined,
        messages: [],
        user_messages: [],
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
    chat_messages() {
      // Return the message in reverse order, i.e. newest first
      return this.chat.messages.reverse();
    },
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
  },
  methods: {
    async getAppInfo() {
      const resp = await fetch(this.info_endpoint, {
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
      if (this.form_email && this.form_name) {
        // Disable the form
        this.submitted = true;
        // Trim strings
        let trimmed_name = this.form_name.trim();
        let trimmed_email = this.form_email.trim();
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
        // Set the chat session info
        this.chat.name = trimmed_name;
        this.chat.email = trimmed_email;
        // Subscribe to the channel
        this.chat.channel = this.pusher.subscribe("private-" + user_info.email);
        this.logged_in = true;
      }
    },
    sendMessage() {
      if (this.chat.channel) {
        // Disable the input
        this.disable_input = true;

        // Create the message
        const createdAt = new Date().toUTCString();
        const message = {
          sender: this.chat.name,
          email: this.chat.email,
          text: this.text_input,
          createdAt: createdAt,
        };

        // Send the message to the channel
        this.chat.channel.trigger("client-guest-new-message", message);

        // Add the message to the user's messages
        this.chat.user_messages.push({
          text: message.text,
          name: message.sender,
          sender: message.email,
        });

        // Clear the input
        this.text_input = "";

        // Enable the input
        this.disable_input = false;
        console.log("Message sent");
      }
    },
    async newMessage(message) {
      // Expecting {text: "", name: "", sender: ""}
      console.log("New message received");
      console.log(message);
      if (message !== undefined) {
        // Add the message to the chat
        let resolved_message = await message;
        let receivedAt = new Date().toUTCString();
        this.chat.messages.push({
          ...resolved_message,
          receivedAt: receivedAt,
        });
      }
    },
  },
};
</script>

<style scoped></style>
