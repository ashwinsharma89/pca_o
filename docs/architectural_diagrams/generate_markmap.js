const fs = require('fs');
const { Transformer } = require('markmap-lib');
const { fillTemplate } = require('markmap-render');
const path = require('path');

async function generateMarkmaps() {
    const transformer = new Transformer();

    // Directory containing this script and the markdown files
    const dir = __dirname;

    // Find all files starting with 'markmap_' and ending with '.md'
    const files = fs.readdirSync(dir).filter(file => file.startsWith('markmap_') && file.endsWith('.md'));

    console.log(`Found ${files.length} markmap files to process.`);

    for (const file of files) {
        const inputPath = path.join(dir, file);
        const outputPath = path.join(dir, file.replace('.md', '.html'));

        console.log(`Processing ${file}...`);

        try {
            // Read Markdown
            const markdown = fs.readFileSync(inputPath, 'utf-8');

            // Transform
            const { root, features } = transformer.transform(markdown);

            // Get assets
            const assets = transformer.getUsedAssets(features);

            // Render HTML
            const html = fillTemplate(root, assets);

            // Save
            fs.writeFileSync(outputPath, html);
            console.log(`✅ Generated ${path.basename(outputPath)}`);

        } catch (err) {
            console.error(`❌ Failed to process ${file}:`, err);
        }
    }
}

generateMarkmaps().catch(console.error);
