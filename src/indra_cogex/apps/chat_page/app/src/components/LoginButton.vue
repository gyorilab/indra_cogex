<template>
  <button
    class="btn btn-primary"
    @click="user_email ? this.trigger_logout : this.trigger_login"
    id="loginout-button"
    :disabled="!endpointsAvailable"
  >
    {{ user_email ? "Logout" : "Login" }}
  </button>
  <div id="overlay">
    <button class="btn btn-danger" id="x-out">x</button>
    <div id="overlay-form-div">
      <div
        style="
          padding: 1em;
          text-align: center;
          margin-left: auto;
          margin-right: auto;
        "
      >
        <img
          src="https://bigmech.s3.amazonaws.com/indra-db/indralab_bare_logo.png"
          alt="IndraLab Logo"
          width="150em"
        />
        <h1>IndraLab</h1>
      </div>
      <div class="container">
        <nav>
          <div class="nav nav-tabs" id="nav-tab" role="tablist">
            <a
              class="nav-item nav-link active show"
              id="nav-login-tab"
              data-toggle="tab"
              href="#nav-login"
              role="tab"
              aria-controls="nav-login"
              aria-selected="true"
              >Login</a
            >
            <a
              class="nav-item nav-link"
              id="nav-register-tab"
              data-toggle="tab"
              href="#nav-register"
              role="tab"
              aria-controls="nav-register"
              aria-selected="false"
              >Register</a
            >
          </div>
        </nav>
      </div>
      <div class="tab-content" id="overlay-all-forms">
        <div
          class="tab-pane auth-tab fade active show"
          id="nav-login"
          role="tabpanel"
          aria-labelledby="nav-login-tab"
        >
          <form
            class="form overlay-form"
            id="overlay-form-login"
            onsubmit="return false;"
          >
            <input type="hidden" name="type" value="login" />
            email
            <input
              name="email"
              type="email"
              class="form-control"
              placeholder="your@email.com"
            />
            password
            <input
              name="password"
              type="password"
              class="form-control"
              placeholder="password"
            />
            <button
              class="btn btn-primary"
              type="submit"
              id="overlay-button-login"
            >
              Login
            </button>
          </form>
        </div>
        <div
          class="tab-pane auth-tab fade"
          id="nav-register"
          role="tabpanel"
          aria-labelledby="nav-register-tab"
        >
          <form
            class="form overlay-form"
            id="overlay-form-register"
            onsubmit="return false;"
          >
            <input type="hidden" name="type" value="register" />
            email
            <input
              name="email"
              type="email"
              class="form-control"
              placeholder="your@email.com"
            />
            password
            <input
              name="password0"
              type="password"
              class="form-control"
              placeholder="password"
            />
            <input
              name="password1"
              type="password"
              class="form-control"
              placeholder="repeat password"
            />
            <button
              class="btn btn-primary"
              type="submit"
              id="overlay-button-register"
            >
              Register
            </button>
          </form>
        </div>
      </div>
      <small id="overlay-message"></small>
    </div>
  </div>
</template>

<script>
export default {
  name: "LoginButton.vue",
  data() {
    return {
      pusher_info: {},
      user_email: null, // From login
    };
  },
  mounted() {
    this.getAppInfo();
  },
  computed: {
    endpointsAvailable() {
      const hasLogin = this.pusher_info.indralab_login !== undefined;
      const hasRegister = this.pusher_info.indralab_register !== undefined;
      const hasLogout = this.pusher_info.indralab_logout !== undefined;
      console.log(
        "LoginButton.vue: endpointsAvailable: hasLogin:",
        hasLogin,
        "hasRegister:",
        hasRegister,
        "hasLogout:",
        hasLogout
      );
      return hasLogin && hasRegister && hasLogout;
    },
  },
  methods: {
    async getAppInfo() {
      const resp = await fetch(this.$info_endpoint, {
        method: "GET",
      });
      const data = await resp.json();
      this.pusher_info = await data;
    },
    trigger_login() {
      console.log("trigger_login");
      return true;
    },
    trigger_logout() {
      console.log("trigger_logout");
      return true;
    },
    report_login_result(result) {
      console.log("report_login_result", result);
    },
    handle_success(type, resp_data) {
      const user_msg = document.querySelector("#user-loginout-msg"); // fixme Make into vue
      if (type === "login") {
        const btn = document.querySelector("#loginout-button"); // fixme Make into vue
        btn.innerHTML = "Logout";
        btn.onclick = () => {
          return this.trigger_logout();
        };
        document.querySelector("#user-logoin"); // fixme Make into vue
        user_msg.innerHTML = `Welcome, ${resp_data.user_email}`; // fixme Make into vue
        this.report_login_result(""); // clear the login result message // fixme Make into vue
      } else if (type === "register") {
        this.trigger_login(); // fixme Make into vue
      } else {
        // logout
        const btn = document.querySelector("#loginout-button"); // fixme Make into vue
        btn.innerHTML = "Login";
        btn.onclick = () => {
          return this.trigger_login();
        };
        user_msg.innerHTML = "";
      }
    },
    async postAuth(action, json, on_complete) {
      const resp = await fetch(this.authUrls[action], {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(json),
      });
      console.log(resp);
      const resp_json = await resp.json();
      on_complete(resp_json, resp);
    },
    login(successCallback, failureCallback, checked = false) {
      // reimplement login from auth_macros.html
      return checked;
    },
  },
};
</script>

<style scoped>
#overlay {
  position: fixed;
  display: none;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background-color: rgba(0, 0, 0, 0.5);
  z-index: 2048;
  cursor: pointer;
}

#overlay-form-div {
  position: absolute;
  top: 50%;
  left: 50%;
  font-size: 40px;
  color: black;
  background-color: lightgray;
  border-radius: 4px;
  transform: translate(-50%, -50%);
  -ms-transform: translate(-50%, -50%);
  padding: 1em;
  max-width: 15em;
}

.overlay-form {
  padding: 1em;
}

#x-out {
  border-radius: 25px;
  height: 50px;
  width: 50px;
  position: absolute;
  top: 30px;
  right: 30px;
}

.auth-tab {
  background-color: white;
  border-radius: 4px;
}

#overlay-message {
  font-size: 9pt;
  color: #803030;
  max-width: 10em;
}
</style>
