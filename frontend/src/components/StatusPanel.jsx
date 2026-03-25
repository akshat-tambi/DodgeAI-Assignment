export default function StatusPanel({ state, error }) {
  return (
    <div className="panel status-panel">
      <h3>Pipeline Status</h3>
      {!state && <p>No job yet.</p>}
      {state && (
        <>
          <p><strong>Job:</strong> {state.job_id}</p>
          <p><strong>Status:</strong> {state.status}</p>
          <p><strong>Stage:</strong> {state.stage}</p>
          <p><strong>Message:</strong> {state.message}</p>
        </>
      )}
      {error && <p className="error">{error}</p>}
    </div>
  );
}
