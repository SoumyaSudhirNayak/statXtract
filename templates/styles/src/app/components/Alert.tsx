import { CheckCircle, AlertTriangle, Info, Clock } from "lucide-react";

type AlertType = "success" | "warning" | "info" | "scheduled";

interface AlertProps {
  type: AlertType;
  title: string;
  children: React.ReactNode;
}

export function Alert({ type, title, children }: AlertProps) {
  const configs = {
    success: {
      bg: "#E8F5E9",
      border: "#2E7D32",
      icon: CheckCircle,
      iconColor: "#2E7D32",
      titleColor: "#2E7D32",
      textColor: "#1b5e20",
    },
    warning: {
      bg: "#FFF3CD",
      border: "#F4A300",
      icon: AlertTriangle,
      iconColor: "#F4A300",
      titleColor: "#7a4f00",
      textColor: "#7a4f00",
    },
    info: {
      bg: "#EBF1FA",
      border: "#2E5BBA",
      icon: Info,
      iconColor: "#2E5BBA",
      titleColor: "#2E5BBA",
      textColor: "#1a3a6e",
    },
    scheduled: {
      bg: "#EBF1FA",
      border: "#2E5BBA",
      icon: Clock,
      iconColor: "#2E5BBA",
      titleColor: "#2E5BBA",
      textColor: "#1a3a6e",
    },
  };

  const config = configs[type];
  const Icon = config.icon;

  return (
    <div
      className="rounded-lg p-4 border-l-4"
      style={{
        backgroundColor: config.bg,
        borderLeftColor: config.border,
      }}
    >
      <div className="flex gap-3">
        <Icon className="w-5 h-5 flex-shrink-0 mt-0.5" style={{ color: config.iconColor }} />
        <div className="flex-1">
          <h4
            className="m-0 mb-2"
            style={{
              fontFamily: "var(--font-body)",
              fontSize: "14px",
              fontWeight: 600,
              color: config.titleColor,
            }}
          >
            {title}
          </h4>
          <p
            className="m-0"
            style={{
              fontFamily: "var(--font-body)",
              fontSize: "14px",
              fontWeight: 400,
              lineHeight: 1.6,
              color: config.textColor,
            }}
          >
            {children}
          </p>
        </div>
      </div>
    </div>
  );
}
