import { Header } from "../../components/Header";
import { Navigation } from "../../components/Navigation";
import { BackButton } from "../../components/BackButton";
import { DashboardCard } from "../../components/DashboardCard";
import { AnimatedBackground } from "../../components/AnimatedBackground";
import {
  Upload,
  Database,
  Download,
  Search,
  Table,
  FileText,
  Settings,
  Key,
  Users,
  Cog,
  List,
} from "lucide-react";

export function AdminDashboard() {
  const adminData = JSON.parse(sessionStorage.getItem("adminData") || '{"fullName":"Admin"}');

  const borderColors = [
    "#2E5BBA", // Blue
    "#2E7D32", // Green
    "#F4A300", // Orange
    "#4A90E2", // Light Blue
    "#D32F2F", // Red
    "#7B1FA2", // Purple
    "#1976D2", // Medium Blue
    "#2E7D32", // Green
    "#F57C00", // Dark Orange
    "#2E5BBA", // Blue
    "#4A90E2", // Light Blue
  ];

  const dashboardCards = [
    {
      title: "Upload Dataset",
      description: "Upload CSV datasets to the system",
      icon: Upload,
      path: "/admin/upload-dataset",
    },
    {
      title: "Nesstar Upload",
      description: "Upload data through Nesstar format",
      icon: Database,
      path: "/admin/nesta-upload",
    },
    {
      title: "NADA Import",
      description: "Import datasets from NADA repository",
      icon: Download,
      path: "/admin/nada-import",
    },
    {
      title: "Query Data",
      description: "Execute queries and view results",
      icon: Search,
      path: "/admin/query-data",
    },
    {
      title: "View Schema & Tables",
      description: "Browse database schemas and table structures",
      icon: Table,
      path: "/admin/view-schema",
    },
    {
      title: "Usage Logs",
      description: "Monitor user activity and data access",
      icon: List,
      path: "/admin/usage-logs",
    },
    {
      title: "Configure Variables",
      description: "Set up and manage data variables",
      icon: Settings,
      path: "/admin/configure-variables",
    },
    {
      title: "Metadata",
      description: "View and edit dataset metadata",
      icon: FileText,
      path: "/admin/metadata",
    },
    {
      title: "Change Password",
      description: "Update your account password",
      icon: Key,
      path: "/admin/change-password",
    },
    {
      title: "User Management",
      description: "Manage user accounts and permissions",
      icon: Users,
      path: "/admin/user-management",
    },
    {
      title: "System Settings",
      description: "Configure system-wide settings",
      icon: Cog,
      path: "/admin/system-settings",
    },
  ];

  return (
    <div className="min-h-screen relative">
      <AnimatedBackground />
      
      <div className="relative z-10">
        <Header userName={adminData.fullName} userRole="admin" />
        <Navigation userRole="admin" />

        <div className="max-w-[1800px] mx-auto px-10 py-12">
          <div className="flex justify-between items-center mb-12">
            <BackButton to="/admin/auth" />
            <h1 className="text-4xl text-[#1C1C1C] m-0" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>Admin Dashboard</h1>
            <div className="w-[80px]"></div> {/* Spacer for alignment */}
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-2 xl:grid-cols-3 gap-10">
            {dashboardCards.map((card, index) => (
              <DashboardCard key={index} {...card} borderColor={borderColors[index]} />
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}