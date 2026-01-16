from PIL import Image
import os

def generate_pdf():
    # Define file order
    files = [
        "chart_a_architecture.png",
        "chart_b_agent_swarm.png",
        "chart_c_data_physics.png",
        "chart_d_layer_latency.png",
        "chart_e_libraries.png",
        "chart_f_full_stack.png"
    ]
    
    base_dir = "docs/architectural_diagrams"
    output_path = os.path.join(base_dir, "PCA_Agent_Architecture_Report.pdf")
    
    images = []
    
    print("Loading images...")
    for filename in files:
        path = os.path.join(base_dir, filename)
        if os.path.exists(path):
            try:
                # Open and convert to RGB (necessary for PDF)
                img = Image.open(path)
                if img.mode == 'RGBA':
                    img = img.convert('RGB')
                images.append(img)
                print(f"✅ Loaded {filename}")
            except Exception as e:
                print(f"❌ Failed to load {filename}: {e}")
        else:
            print(f"⚠️ File not found: {filename}")
            
    if images:
        print(f"Generating PDF with {len(images)} pages...")
        # Save first image and append the rest
        first_image = images[0]
        rest_images = images[1:]
        
        first_image.save(
            output_path, 
            "PDF", 
            resolution=100.0, 
            save_all=True, 
            append_images=rest_images
        )
        print(f"🎉 PDF generated: {output_path}")
    else:
        print("No images found to generate PDF.")

if __name__ == "__main__":
    generate_pdf()
