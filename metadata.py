#!/usr/bin/env python3
"""
Generate Metadata for Existing Videos

This script scans your blob storage for existing videos and creates metadata files.
It can work with videos already uploaded to Azure Blob Storage.
"""

import os
import json
import re
from pathlib import Path
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from typing import Dict, List
from urllib.parse import quote

class MetadataGenerator:
    def __init__(self, connection_string: str, container_name: str = "training-videos"):
        self.blob_service = BlobServiceClient.from_connection_string(connection_string)
        self.container_name = container_name
        self.container_client = self.blob_service.get_container_client(container_name)
        
    def list_existing_videos(self) -> List[Dict]:
        """List all video files in the blob storage."""
        video_extensions = {'.mp4', '.avi', '.mov', '.wmv', '.mkv', '.flv', '.webm'}
        videos = []
        
        print("🔍 Scanning for existing videos...")
        
        try:
            blobs = self.container_client.list_blobs()
            for blob in blobs:
                # Skip metadata folder
                if blob.name.startswith('metadata/'):
                    continue
                    
                # Check if it's a video file
                file_path = Path(blob.name)
                if file_path.suffix.lower() in video_extensions:
                    # Determine category and clean name
                    category, clean_name = self.parse_video_info(blob.name)
                    
                    video_info = {
                        'blob_name': blob.name,
                        'file_name': file_path.name,
                        'clean_name': clean_name,
                        'category': category,
                        'size': blob.size,
                        'url': f"https://{self.blob_service.account_name}.blob.core.windows.net/{self.container_name}/{quote(blob.name)}"
                    }
                    videos.append(video_info)
            
            print(f"📹 Found {len(videos)} video files")
            return videos
            
        except Exception as e:
            print(f"❌ Error listing videos: {e}")
            return []
    
    def parse_video_info(self, blob_name: str) -> tuple:
        """Parse video info from blob name to determine category and clean name."""
        file_path = Path(blob_name)
        file_name = file_path.stem
        
        # Check if it's in a folder structure (beginner/video.mp4)
        if '/' in blob_name:
            parts = blob_name.split('/')
            if len(parts) >= 2 and parts[0].lower() in ['beginner', 'intermediate', 'advanced']:
                category = parts[0].lower()
                clean_name = Path(parts[-1]).stem
                return category, clean_name
        
        # Check if it has category prefix (beginner-video.mp4)
        for cat in ['beginner', 'intermediate', 'advanced']:
            if file_name.lower().startswith(f"{cat}-"):
                clean_name = file_name[len(cat)+1:]  # Remove "category-" prefix
                return cat, clean_name
        
        # Auto-detect from filename content
        category = self.detect_category_from_filename(file_name)
        return category, file_name
    
    def detect_category_from_filename(self, filename: str) -> str:
        """Auto-detect difficulty category from filename."""
        filename_lower = filename.lower()
        
        if any(word in filename_lower for word in ['beginner', 'basic', 'intro', 'fundamentals', '101', 'start']):
            return 'beginner'
        elif any(word in filename_lower for word in ['advanced', 'expert', 'master', 'professional', 'deep']):
            return 'advanced'
        elif any(word in filename_lower for word in ['intermediate', 'medium', 'standard', 'level2']):
            return 'intermediate'
        else:
            return 'beginner'  # Default
    
    def create_metadata(self, video_info: Dict, custom_data: Dict = None) -> Dict:
        """Create comprehensive metadata for a video."""
        clean_name = video_info['clean_name']
        category = video_info['category']
        
        # Clean up name for title
        title = self.clean_filename_for_title(clean_name)
        
        # Generate video ID
        video_id = f"{category}-{clean_name.lower().replace(' ', '-').replace('_', '-')}"
        
        # Base metadata
        metadata = {
            "video_id": video_id,
            "title": title,
            "description": f"Training video: {title} - {category.capitalize()} level content for comprehensive learning",
            "category": category,
            "duration_seconds": self.estimate_duration(video_info['size']),
            "thumbnail_url": "",  # You can add thumbnail generation later
            "video_url": video_info['url'],
            "transcript": f"Training content covering {title.lower()}. This {category}-level course provides comprehensive instruction on the topic.",
            "topics": self.generate_topics_from_title(title),
            "learning_objectives": self.generate_learning_objectives(title, category),
            "prerequisites": self.generate_prerequisites(category),
            "difficulty_level": category,
            "instructor": "Training Expert",
            "created_date": "2024-10-21T00:00:00Z",
            "updated_date": "2024-10-21T00:00:00Z",
            "tags": self.generate_tags(title, category),
            "chapters": self.generate_chapters(title, category)
        }
        
        # Merge custom data if provided
        if custom_data:
            metadata.update(custom_data)
        
        return metadata
    
    def clean_filename_for_title(self, filename: str) -> str:
        """Convert filename to proper title."""
        # Remove common prefixes and clean up
        title = re.sub(r'^(beginner|intermediate|advanced)[-_]?', '', filename, flags=re.IGNORECASE)
        title = re.sub(r'[-_]+', ' ', title)
        title = re.sub(r'\d+', '', title)  # Remove numbers
        
        # Capitalize properly
        title = ' '.join(word.capitalize() for word in title.split() if word)
        
        return title or "Training Video"
    
    def estimate_duration(self, file_size: int) -> int:
        """Estimate video duration based on file size (rough approximation)."""
        # Very rough estimate: 1MB ≈ 8 seconds for standard quality
        estimated_seconds = max(300, min(7200, file_size // (1024 * 1024) * 8))
        return estimated_seconds
    
    def generate_topics_from_title(self, title: str) -> List[str]:
        """Generate relevant topics."""
        words = [word.lower() for word in title.split() if len(word) > 3]
        topics = ['training', 'tutorial'] + words[:3]  # Limit to avoid too many topics
        return list(set(topics))
    
    def generate_learning_objectives(self, title: str, category: str) -> List[str]:
        """Generate learning objectives."""
        objectives_map = {
            'beginner': [
                f"Understand the fundamentals of {title.lower()}",
                "Apply basic concepts in practical scenarios",
                "Identify key principles and best practices"
            ],
            'intermediate': [
                f"Master intermediate techniques in {title.lower()}",
                "Analyze complex scenarios and develop solutions",
                "Integrate concepts with existing knowledge"
            ],
            'advanced': [
                f"Achieve expert-level proficiency in {title.lower()}",
                "Develop innovative approaches and strategies",
                "Lead implementation and mentor others"
            ]
        }
        return objectives_map.get(category, objectives_map['beginner'])
    
    def generate_prerequisites(self, category: str) -> List[str]:
        """Generate prerequisites based on level."""
        prereq_map = {
            'beginner': [],
            'intermediate': ["Basic understanding of core concepts", "Completion of beginner-level training"],
            'advanced': ["Intermediate certification", "Practical experience", "Strong foundational knowledge"]
        }
        return prereq_map.get(category, [])
    
    def generate_tags(self, title: str, category: str) -> List[str]:
        """Generate tags."""
        base_tags = ['training', 'tutorial', category]
        title_words = [word.lower() for word in title.split() if len(word) > 3]
        return list(set(base_tags + title_words[:3]))  # Limit tags
    
    def generate_chapters(self, title: str, category: str) -> List[Dict]:
        """Generate chapter structure."""
        if category == 'beginner':
            return [
                {"title": "Course Introduction", "start_time": 0, "end_time": 180, "description": f"Welcome and overview of {title}"},
                {"title": "Basic Concepts", "start_time": 180, "end_time": 900, "description": "Fundamental principles and concepts"},
                {"title": "Practical Examples", "start_time": 900, "end_time": 1200, "description": "Step-by-step examples and demonstrations"},
                {"title": "Practice & Summary", "start_time": 1200, "end_time": 1500, "description": "Practice exercises and key takeaways"}
            ]
        elif category == 'intermediate':
            return [
                {"title": "Review & Setup", "start_time": 0, "end_time": 300, "description": "Prerequisites review and setup"},
                {"title": "Advanced Concepts", "start_time": 300, "end_time": 1000, "description": "Intermediate-level concepts and techniques"},
                {"title": "Case Studies", "start_time": 1000, "end_time": 1400, "description": "Real-world applications and case studies"},
                {"title": "Best Practices", "start_time": 1400, "end_time": 1800, "description": "Industry best practices and next steps"}
            ]
        else:  # advanced
            return [
                {"title": "Expert Overview", "start_time": 0, "end_time": 240, "description": "Advanced concepts overview"},
                {"title": "Deep Dive Analysis", "start_time": 240, "end_time": 1080, "description": "In-depth technical analysis"},
                {"title": "Advanced Techniques", "start_time": 1080, "end_time": 1560, "description": "Expert-level techniques and strategies"},
                {"title": "Innovation & Leadership", "start_time": 1560, "end_time": 1800, "description": "Innovation approaches and leadership aspects"}
            ]
    
    def upload_metadata(self, metadata: Dict, metadata_filename: str):
        """Upload metadata to blob storage."""
        try:
            metadata_json = json.dumps(metadata, indent=2)
            blob_path = f"metadata/{metadata_filename}"
            
            blob_client = self.container_client.get_blob_client(blob_path)
            blob_client.upload_blob(metadata_json, overwrite=True)
            
            print(f"✅ Created metadata: {blob_path}")
            return True
            
        except Exception as e:
            print(f"❌ Error uploading metadata {metadata_filename}: {e}")
            return False
    
    def generate_all_metadata(self, force_overwrite: bool = False):
        """Generate metadata for all videos in blob storage."""
        videos = self.list_existing_videos()
        
        if not videos:
            print("❌ No videos found in blob storage")
            return
        
        print(f"\n📝 Generating metadata for {len(videos)} videos...")
        
        # Check existing metadata files
        existing_metadata = set()
        try:
            metadata_blobs = self.container_client.list_blobs(name_starts_with="metadata/")
            for blob in metadata_blobs:
                if blob.name.endswith('.json'):
                    filename = Path(blob.name).stem
                    existing_metadata.add(filename)
        except:
            pass
        
        generated_count = 0
        skipped_count = 0
        
        for video in videos:
            metadata_filename = f"{video['clean_name']}.json"
            
            # Skip if metadata already exists (unless force overwrite)
            if video['clean_name'] in existing_metadata and not force_overwrite:
                print(f"⏭️  Skipping {video['file_name']} (metadata exists)")
                skipped_count += 1
                continue
            
            # Generate metadata
            print(f"📝 Generating metadata for: {video['file_name']}")
            metadata = self.create_metadata(video)
            
            # Upload metadata
            if self.upload_metadata(metadata, metadata_filename):
                generated_count += 1
            
        print(f"\n✅ Metadata generation complete!")
        print(f"   📝 Generated: {generated_count}")
        print(f"   ⏭️  Skipped: {skipped_count}")
        print(f"   📁 Total videos: {len(videos)}")
    
    def generate_metadata_for_video(self, video_blob_name: str, custom_metadata: Dict = None):
        """Generate metadata for a specific video."""
        videos = self.list_existing_videos()
        target_video = None
        
        for video in videos:
            if video['blob_name'] == video_blob_name or video['file_name'] == video_blob_name:
                target_video = video
                break
        
        if not target_video:
            print(f"❌ Video not found: {video_blob_name}")
            return
        
        print(f"📝 Generating metadata for: {target_video['file_name']}")
        metadata = self.create_metadata(target_video, custom_metadata)
        
        metadata_filename = f"{target_video['clean_name']}.json"
        self.upload_metadata(metadata, metadata_filename)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate metadata for existing videos in blob storage")
    parser.add_argument("--env-file", default=".env", help="Environment file path")
    parser.add_argument("--container", default="training-videos", help="Blob container name")
    parser.add_argument("--force", action="store_true", help="Overwrite existing metadata files")
    parser.add_argument("--video", help="Generate metadata for specific video only")
    parser.add_argument("--list-only", action="store_true", help="Only list videos, don't generate metadata")
    
    args = parser.parse_args()
    
    # Load environment
    load_dotenv(args.env_file)
    
    connection_string = os.getenv("BLOB_CONNECTION_STRING")
    if not connection_string:
        print("❌ Error: BLOB_CONNECTION_STRING not found in environment")
        return
    
    # Initialize generator
    generator = MetadataGenerator(connection_string, args.container)
    
    if args.list_only:
        # Just list videos
        videos = generator.list_existing_videos()
        print(f"\n📋 Videos in {args.container}:")
        for video in videos:
            print(f"   📹 {video['file_name']} ({video['category']}) - {video['size']:,} bytes")
    
    elif args.video:
        # Generate metadata for specific video
        generator.generate_metadata_for_video(args.video)
    
    else:
        # Generate metadata for all videos
        generator.generate_all_metadata(args.force)


if __name__ == "__main__":
    main()