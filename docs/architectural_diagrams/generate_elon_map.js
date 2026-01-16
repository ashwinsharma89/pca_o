const fs = require('fs');
const { Transformer } = require('markmap-lib');
const { fillTemplate } = require('markmap-render');

async function generateElonMap() {
    const markdown = fs.readFileSync('docs/architectural_diagrams/elon_detailed_architecture.md', 'utf-8');
    const transformer = new Transformer();
    const { root, features } = transformer.transform(markdown);
    const assets = transformer.getUsedAssets(features);
    const html = fillTemplate(root, assets);
    fs.writeFileSync('docs/architectural_diagrams/elon_detailed_architecture.html', html);
    console.log('Elon Map generated at docs/architectural_diagrams/elon_detailed_architecture.html');
}

generateElonMap().catch(console.error);
