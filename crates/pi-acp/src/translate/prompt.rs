//! ACP content blocks -> pi message + images.
//!
//! Ports `acp/translate/prompt.ts`. ACP `session/prompt` content blocks are
//! flattened into the single text message pi's `prompt` command takes, with
//! images carried separately (pi-ai `ImageContent`). Unsupported block types
//! are rendered as explicit human-readable markers so context is never
//! silently dropped.

use agent_client_protocol::schema::v1::{ContentBlock, EmbeddedResourceResource};

/// A pi image attachment (`ImageContent`, base64 without data-url prefix).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PiImage {
    /// MIME type, e.g. `image/png`.
    pub mime_type: String,
    /// Base64-encoded image bytes.
    pub data: String,
}

/// The flattened pi `prompt` payload for an ACP prompt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PiPrompt {
    pub message: String,
    pub images: Vec<PiImage>,
}

/// Convert ACP prompt content blocks into a pi message + images.
///
/// Mirrors TS `promptToPiMessage`:
/// - `text` blocks are concatenated verbatim;
/// - `resource_link` becomes a lightweight `\n[Context] <uri>` hint;
/// - `image` blocks become pi image attachments (not text);
/// - `resource` blocks become `\n[Embedded Context] ...` markers (text or blob);
/// - `audio` becomes a `\n[Audio] ... not supported by pi-acp` marker.
pub fn prompt_to_pi_message(blocks: &[ContentBlock]) -> PiPrompt {
    let mut message = String::new();
    let mut images: Vec<PiImage> = Vec::new();

    for block in blocks {
        match block {
            ContentBlock::Text(t) => message.push_str(&t.text),

            ContentBlock::ResourceLink(link) => {
                message.push_str(&format!("\n[Context] {}", link.uri));
            }

            ContentBlock::Image(img) => {
                images.push(PiImage {
                    mime_type: img.mime_type.clone(),
                    data: img.data.clone(),
                });
            }

            ContentBlock::Resource(resource) => {
                // Clients should not send this when embeddedContext=false, but
                // be resilient.
                match &resource.resource {
                    EmbeddedResourceResource::TextResourceContents(text) => {
                        let mime = text
                            .mime_type
                            .clone()
                            .unwrap_or_else(|| "text/plain".to_string());
                        message.push_str(&format!(
                            "\n[Embedded Context] {} ({mime})\n{}",
                            text.uri, text.text
                        ));
                    }
                    EmbeddedResourceResource::BlobResourceContents(blob) => {
                        let mime = blob
                            .mime_type
                            .clone()
                            .unwrap_or_else(|| "application/octet-stream".to_string());
                        let bytes = base64_decoded_len(&blob.blob);
                        message.push_str(&format!(
                            "\n[Embedded Context] {} ({mime}, {bytes} bytes)",
                            blob.uri
                        ));
                    }
                    // Future resource kinds (protocol evolution) are ignored.
                    _ => {}
                }
            }

            ContentBlock::Audio(audio) => {
                let bytes = base64_decoded_len(&audio.data);
                message.push_str(&format!(
                    "\n[Audio] ({}, {bytes} bytes) not supported by pi-acp",
                    audio.mime_type
                ));
            }

            // Unknown block types (protocol evolution) are ignored, matching
            // the TS reference.
            _ => {}
        }
    }

    PiPrompt { message, images }
}

/// Number of decoded bytes a base64 payload carries without decoding it
/// (mirrors `Buffer.byteLength(data, 'base64')`; lenient on malformed input).
fn base64_decoded_len(data: &str) -> usize {
    let chars = data
        .bytes()
        .filter(|&b| b != b'=' && !b.is_ascii_whitespace())
        .count();
    (chars / 4) * 3
        + match chars % 4 {
            2 => 1,
            3 => 2,
            _ => 0,
        }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::{
        AudioContent, BlobResourceContents, EmbeddedResource, ImageContent, ResourceLink,
        TextContent, TextResourceContents,
    };

    fn text(s: &str) -> ContentBlock {
        ContentBlock::Text(TextContent::new(s))
    }

    #[test]
    fn concatenates_text_and_resource_links() {
        let out = prompt_to_pi_message(&[
            text("Hello"),
            ContentBlock::ResourceLink(ResourceLink::new("foo", "file:///tmp/foo.txt")),
            text(" world"),
        ]);
        assert_eq!(out.message, "Hello\n[Context] file:///tmp/foo.txt world");
        assert!(out.images.is_empty());
    }

    #[test]
    fn empty_blocks_produce_empty_prompt() {
        let out = prompt_to_pi_message(&[]);
        assert_eq!(out.message, "");
        assert!(out.images.is_empty());
    }

    #[test]
    fn embedded_resource_text_becomes_marker() {
        let out = prompt_to_pi_message(&[ContentBlock::Resource(EmbeddedResource::new(
            EmbeddedResourceResource::TextResourceContents(
                TextResourceContents::new("hi", "file:///tmp/a.txt").mime_type("text/plain"),
            ),
        ))]);
        assert_eq!(
            out.message,
            "\n[Embedded Context] file:///tmp/a.txt (text/plain)\nhi"
        );
        assert!(out.images.is_empty());
    }

    #[test]
    fn embedded_resource_defaults_mime_to_text_plain() {
        let out = prompt_to_pi_message(&[ContentBlock::Resource(EmbeddedResource::new(
            EmbeddedResourceResource::TextResourceContents(TextResourceContents::new(
                "hi",
                "file:///tmp/a.txt",
            )),
        ))]);
        assert_eq!(
            out.message,
            "\n[Embedded Context] file:///tmp/a.txt (text/plain)\nhi"
        );
    }

    #[test]
    fn embedded_resource_blob_reports_byte_count() {
        // "xyz" base64-encoded
        let blob = "eHl6";
        let out = prompt_to_pi_message(&[ContentBlock::Resource(EmbeddedResource::new(
            EmbeddedResourceResource::BlobResourceContents(
                BlobResourceContents::new(blob, "file:///tmp/a.bin")
                    .mime_type("application/octet-stream"),
            ),
        ))]);
        assert_eq!(
            out.message,
            "\n[Embedded Context] file:///tmp/a.bin (application/octet-stream, 3 bytes)"
        );
    }

    #[test]
    fn audio_becomes_unsupported_marker() {
        // "abc" base64-encoded
        let data = "YWJj";
        let out =
            prompt_to_pi_message(&[ContentBlock::Audio(AudioContent::new(data, "audio/wav"))]);
        assert_eq!(
            out.message,
            "\n[Audio] (audio/wav, 3 bytes) not supported by pi-acp"
        );
        assert!(out.images.is_empty());
    }

    #[test]
    fn image_maps_to_pi_image_content() {
        // "abc" base64-encoded
        let data = "YWJj";
        let out = prompt_to_pi_message(&[
            text("see"),
            ContentBlock::Image(ImageContent::new(data, "image/png")),
        ]);
        assert_eq!(out.message, "see");
        assert_eq!(out.images.len(), 1);
        assert_eq!(
            out.images[0],
            PiImage {
                mime_type: "image/png".to_string(),
                data: data.to_string(),
            }
        );
    }

    #[test]
    fn base64_decoded_lengths() {
        assert_eq!(base64_decoded_len(""), 0);
        assert_eq!(base64_decoded_len("YQ=="), 1);
        assert_eq!(base64_decoded_len("YWJj"), 3);
        assert_eq!(base64_decoded_len("YWJjZA=="), 4);
        assert_eq!(base64_decoded_len("YWJjZGVm"), 6);
        // padding ignored; whitespace tolerated
        assert_eq!(base64_decoded_len("Y Q = ="), 1);
    }
}
