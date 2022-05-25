To properly import components from indralab-vue:

- Run `npm run build`; this will create a `dist` directory
- Run `npm run pack`; this will package the project in tgz file

Then in the `indra_cogex/apps/chat_page/app` install the package:

```sh
npm install /path/to/project/project-0.0.1.tgz
```

It seems that when componentes from the library are nested, the nested/child components are not resolved. It's unclear at the time why this is but a workaround is to register all the necessary components globally:

```js
import { createApp } from "vue";

import { Statement } from "indralab-vue";
import { Evidence } from "indralab-vue";
import { CurationRow } from "indralab-vue";
import { RefLink } from "indralab-vue";
import { SourceDisplay } from "indralab-vue";
import App from "./App.vue";

const app = createApp(App);

app.component("Statement", Statement);
app.component("Evidence", Evidence);
app.component("RefLink", RefLink);
app.component("SourceDisplay", SourceDisplay);
app.component("CurationRow", CurationRow);


app.mount("#app");
```
