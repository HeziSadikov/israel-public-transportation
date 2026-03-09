import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";

class ErrorBoundary extends React.Component<
  { children: React.ReactNode },
  { hasError: boolean; error: Error | null }
> {
  state = { hasError: false, error: null as Error | null };

  static getDerivedStateFromError(error: Error) {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, info: React.ErrorInfo) {
    console.error("App error:", error, info.componentStack);
  }

  render() {
    if (this.state.hasError && this.state.error) {
      return (
        <div
          style={{
            padding: 24,
            fontFamily: "Arial, sans-serif",
            maxWidth: 600,
            margin: "0 auto",
          }}
        >
          <h1 style={{ color: "#b91c1c" }}>Something went wrong</h1>
          <pre
            style={{
              background: "#fef2f2",
              padding: 16,
              borderRadius: 8,
              overflow: "auto",
              fontSize: 14,
            }}
          >
            {this.state.error.message}
          </pre>
          <p style={{ color: "#6b7280", fontSize: 14 }}>
            Check the browser console (F12) for more details.
          </p>
        </div>
      );
    }
    return this.props.children;
  }
}

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <ErrorBoundary>
      <App />
    </ErrorBoundary>
  </React.StrictMode>
);
