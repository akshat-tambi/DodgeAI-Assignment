const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000/api';

export async function uploadZip(file) {
  const formData = new FormData();
  formData.append('file', file);

  const response = await fetch(`${API_BASE}/upload`, {
    method: 'POST',
    body: formData,
  });

  if (!response.ok) {
    const err = await response.json().catch(() => ({}));
    throw new Error(err.detail || 'Upload failed');
  }

  return response.json();
}

export async function getJob(jobId) {
  const response = await fetch(`${API_BASE}/jobs/${jobId}`);
  if (!response.ok) {
    const err = await response.json().catch(() => ({}));
    throw new Error(err.detail || 'Unable to fetch job');
  }
  return response.json();
}

export async function getGraph(view = 'granular', jobId = '') {
  const params = new URLSearchParams({ view });
  if (jobId) params.set('job_id', jobId);
  const response = await fetch(`${API_BASE}/graph?${params.toString()}`);
  if (!response.ok) {
    const err = await response.json().catch(() => ({}));
    throw new Error(err.detail || 'Unable to fetch graph');
  }
  return response.json();
}

export async function queryChat({ jobId, question, conversationId, selectedNodeId }) {
  const response = await fetch(`${API_BASE}/chat`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      job_id: jobId,
      question,
      conversation_id: conversationId || null,
      selected_node_id: selectedNodeId || null,
    }),
  });

  if (!response.ok) {
    const err = await response.json().catch(() => ({}));
    throw new Error(err.detail || 'Unable to run chat query');
  }

  return response.json();
}
