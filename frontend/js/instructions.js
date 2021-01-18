/**
 * Function to render instructions screen html
 *
 * @return {null}
 */
const renderInstructionsHtml = () => {
  setActiveSelectedMenu('instructions');
  jQuery('#app').load('./instructions.html');
}