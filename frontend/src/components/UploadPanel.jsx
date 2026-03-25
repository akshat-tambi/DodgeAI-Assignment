import { useRef } from 'react';

export default function UploadPanel({ onUpload, disabled }) {
  const inputRef = useRef(null);

  const handleChange = async (event) => {
    const file = event.target.files?.[0];
    if (!file) return;
    await onUpload(file);
    event.target.value = '';
  };

  return (
    <div className="panel upload-panel">
      <h2>Upload Dataset ZIP</h2>
      <p>Upload a ZIP containing folder-per-entity JSONL files.</p>
      <div className="upload-actions">
        <button
          className="btn"
          disabled={disabled}
          onClick={() => inputRef.current?.click()}
        >
          {disabled ? 'Processing...' : 'Choose ZIP File'}
        </button>
        <input
          ref={inputRef}
          type="file"
          accept=".zip"
          onChange={handleChange}
          hidden
        />
      </div>
    </div>
  );
}
