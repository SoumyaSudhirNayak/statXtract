import { createBrowserRouter } from "react-router";
import { SplashScreen } from "./screens/SplashScreen";
import { ModuleSelection } from "./screens/ModuleSelection";
import { AdminAuth } from "./screens/admin/AdminAuth";
import { AdminSignIn } from "./screens/admin/AdminSignIn";
import { AdminCreateAccount } from "./screens/admin/AdminCreateAccount";
import { AdminDashboard } from "./screens/admin/AdminDashboard";
import { AdminProfile } from "./screens/admin/AdminProfile";
import { UploadDataset } from "./screens/admin/UploadDataset";
import { NestaUpload } from "./screens/admin/NestaUpload";
import { NadaImport } from "./screens/admin/NadaImport";
import { QueryData } from "./screens/admin/QueryData";
import { ViewSchema } from "./screens/admin/ViewSchema";
import { UsageLogs } from "./screens/admin/UsageLogs";
import { ConfigureVariables } from "./screens/admin/ConfigureVariables";
import { Metadata } from "./screens/admin/Metadata";
import { ChangePassword } from "./screens/admin/ChangePassword";
import { UserManagement } from "./screens/admin/UserManagement";
import { SystemSettings } from "./screens/admin/SystemSettings";
import { UserModuleSelection } from "./screens/user/UserModuleSelection";
import { UserSignIn } from "./screens/user/UserSignIn";
import { UserCreateAccount } from "./screens/user/UserCreateAccount";
import { OrganizationTypeSelection } from "./screens/user/OrganizationTypeSelection";
import { StudentVerification } from "./screens/user/StudentVerification";
import { ResearcherVerification } from "./screens/user/ResearcherVerification";
import { PrivateOrgVerification } from "./screens/user/PrivateOrgVerification";
import { AnalystVerification } from "./screens/user/AnalystVerification";
import { UserDashboard } from "./screens/user/UserDashboard";
import { UserProfile } from "./screens/user/UserProfile";

export const router = createBrowserRouter([
  {
    path: "/",
    element: <SplashScreen />,
  },
  {
    path: "/module-selection",
    element: <ModuleSelection />,
  },
  // Admin Routes
  {
    path: "/admin/auth",
    element: <AdminAuth />,
  },
  {
    path: "/admin/sign-in",
    element: <AdminSignIn />,
  },
  {
    path: "/admin/create-account",
    element: <AdminCreateAccount />,
  },
  {
    path: "/admin/dashboard",
    element: <AdminDashboard />,
  },
  {
    path: "/admin/profile",
    element: <AdminProfile />,
  },
  {
    path: "/admin/upload-dataset",
    element: <UploadDataset />,
  },
  {
    path: "/admin/nesta-upload",
    element: <NestaUpload />,
  },
  {
    path: "/admin/nada-import",
    element: <NadaImport />,
  },
  {
    path: "/admin/query-data",
    element: <QueryData />,
  },
  {
    path: "/admin/view-schema",
    element: <ViewSchema />,
  },
  {
    path: "/admin/usage-logs",
    element: <UsageLogs />,
  },
  {
    path: "/admin/configure-variables",
    element: <ConfigureVariables />,
  },
  {
    path: "/admin/metadata",
    element: <Metadata />,
  },
  {
    path: "/admin/change-password",
    element: <ChangePassword />,
  },
  {
    path: "/admin/user-management",
    element: <UserManagement />,
  },
  {
    path: "/admin/system-settings",
    element: <SystemSettings />,
  },
  // User Routes
  {
    path: "/user/module-selection",
    element: <UserModuleSelection />,
  },
  {
    path: "/user/sign-in",
    element: <UserSignIn />,
  },
  {
    path: "/user/create-account",
    element: <UserCreateAccount />,
  },
  {
    path: "/user/organization-type",
    element: <OrganizationTypeSelection />,
  },
  {
    path: "/user/verify/student",
    element: <StudentVerification />,
  },
  {
    path: "/user/verify/researcher",
    element: <ResearcherVerification />,
  },
  {
    path: "/user/verify/private",
    element: <PrivateOrgVerification />,
  },
  {
    path: "/user/verify/analyst",
    element: <AnalystVerification />,
  },
  {
    path: "/user/dashboard",
    element: <UserDashboard />,
  },
  {
    path: "/user/profile",
    element: <UserProfile />,
  },
]);