import "./pages/MainScreen/MainScreen.page.js";
import "./pages/SchemaConversionScreen/SchemaConversionScreen.page.js"
// Home screen component
const HomeComponent = {
    render: () => {
        document.getElementById('app').innerHTML = `<hb-main-screen></hb-main-screen>`;
    }
}

// Edit Schema screen component
const SchemaComponent = {
    render: () => {
        document.getElementById('app').innerHTML = `<hb-schema-conversion-screen></hb-schema-conversion-screen>`
    }
}

// Instructions Component
const InstructionsComponent = {
    render: () => jQuery('#app').load('./js/pages/Instructions/Instructions.html')
}

// Error component (for any unrecognized path)
const ErrorComponent = {
    render: () => {
        return `
      <section>
        <h1>Error</h1>
      </section>
    `;
    }
}

// Pre defined paths
const paths = {
    defaultPath: '/',
    schemaReport: '/schema-report',
    instructions: '/instructions'
}

// Pre defined routes
const routes = [
    { path: paths.defaultPath, component: HomeComponent, },
    { path: paths.schemaReport, component: SchemaComponent, },
    { path: paths.instructions, component: InstructionsComponent, }
];

// function to fetch browser url
const parseLocation = () => location.hash.slice(1).toLowerCase() || paths.defaultPath;

// function to find component based on browser url
const findComponentByPath = (path, routes) => {
    return routes.find(r => {
        return r.path.match(new RegExp(`^\\${path}$`, 'gm'))
    }) || undefined;
}

// function to render the html based on path
const router = () => {
    const path = parseLocation();
    const { component = ErrorComponent } = findComponentByPath(path, routes) || {};
    component.render();
};

window.addEventListener('hashchange', router);
window.addEventListener('load', router);