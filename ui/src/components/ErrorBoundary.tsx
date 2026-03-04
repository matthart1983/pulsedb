import { Component, type ReactNode } from 'react'

interface Props {
  children: ReactNode
  fallback?: ReactNode
}

interface State {
  hasError: boolean
  error: string | null
}

export class ErrorBoundary extends Component<Props, State> {
  state: State = { hasError: false, error: null }

  static getDerivedStateFromError(error: Error) {
    return { hasError: true, error: error.message }
  }

  render() {
    if (this.state.hasError) {
      return this.props.fallback ?? (
        <div className="flex items-center justify-center h-full p-4 text-center">
          <div>
            <div className="text-xs text-chart-red mb-1">Render error</div>
            <div className="text-[10px] text-pulse-text-muted">{this.state.error}</div>
            <button
              onClick={() => this.setState({ hasError: false, error: null })}
              className="mt-2 text-[10px] text-chart-blue hover:underline"
            >
              Retry
            </button>
          </div>
        </div>
      )
    }
    return this.props.children
  }
}
